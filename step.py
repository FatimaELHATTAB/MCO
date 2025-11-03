# rapidfuzzy_matcher.py
from __future__ import annotations

import unicodedata
import re
from functools import lru_cache
from typing import List, Dict, Optional, Tuple
import numpy as np
import polars as pl
from rapidfuzz import process, fuzz, distance


# --------- Scorers dispos (références YAML) ----------
SCORER_REGISTRY = {
    "WRatio": fuzz.WRatio,
    "QRatio": fuzz.QRatio,
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "jaro_winkler": distance.JaroWinkler.normalized_similarity,
    "lev": distance.Levenshtein.normalized_similarity,
}


# --------- Cleaning robuste pour noms d'entreprise ----------
class CompanyCleaner:
    _punct_table = str.maketrans({c: " " for c in r"""!"#$%&'()*+,./:;<=>?@[\]^_`{|}~–—-"""} )
    _spaces_re = re.compile(r"\s+")
    _legal_suffix_re = re.compile(
        r"\b("
        r"sa|sas|sarl|eurl|sei|selarl|selas|sc|sasu|se|spa|s\.p\.a|ag|gmbh|kg|mbh|oy|bv|nv|ab|as|oyj|a/s|kft|k\.k\."
        r"|ltd|llc|inc|corp|co|pte|pty|plc|llp|lp"
        r")\b\.?", re.IGNORECASE
    )
    _stopwords_re = re.compile(r"\b(group|holding|company|co|inc|the|and|et|of)\b", re.IGNORECASE)

    @staticmethod
    def _strip_accents(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if not unicodedata.combining(ch))

    @staticmethod
    @lru_cache(maxsize=200_000)
    def clean(token: str) -> str:
        if token is None:
            return ""
        s = str(token).lower().strip()
        if not s:
            return ""
        s = CompanyCleaner._strip_accents(s)
        s = s.translate(CompanyCleaner._punct_table)
        s = s.replace("&", " and ")
        s = CompanyCleaner._legal_suffix_re.sub(" ", s)
        s = CompanyCleaner._stopwords_re.sub(" ", s)
        s = CompanyCleaner._spaces_re.sub(" ", s).strip()
        return s


# --------- Utils vectorisés ----------
def _cdist_col(
    left_vals: List[str],
    right_vals: List[str],
    scorer,
    cutoff: float = 0.0,
    workers: int = -1,
) -> np.ndarray:
    M = process.cdist(left_vals, right_vals, scorer=scorer, score_cutoff=cutoff, workers=workers)
    # fuzz.* -> [0,100], distance.* -> [0,1]
    if M.size and M.max() <= 1.0:
        M = M * 100.0
    return M.astype(float)


def _matrix_to_df(
    S: np.ndarray,
    lblk: pl.DataFrame,
    rblk: pl.DataFrame,
    left_id: str,
    right_id: str,
    min_score: float,
) -> pl.DataFrame:
    if S.size == 0:
        return pl.DataFrame({left_id: [], right_id: [], "score": []})
    rows, cols = np.where(S >= min_score)
    if rows.size == 0:
        return pl.DataFrame({left_id: [], right_id: [], "score": []})
    return pl.DataFrame(
        {
            left_id: lblk[left_id].to_numpy()[rows],
            right_id: rblk[right_id].to_numpy()[cols],
            "score": S[rows, cols].astype(float),
        }
    )


def _topk_per_left(df: pl.DataFrame, k: int) -> pl.DataFrame:
    if df.height == 0:
        return df
    return (
        df.with_columns(pl.col("score").rank(method="dense", descending=True).over("left_id").alias("rk"))
          .filter(pl.col("rk") <= k)
          .drop("rk")
    )


# --------- Matcher principal ----------
class RapidFuzzyMatcher:
    """
    Fuzzy matching vectorisé (RapidFuzz + Polars) avec cleaning agressif.

    Args:
        columns: colonnes côté left
        peers:   mapping left_col -> right_col
        scorers: mapping col -> nom de scorer
        weights: mapping col -> poids
        global_min: seuil final (0..100)
        top_k: meilleurs matches par left_id
        block_by_left / block_by_right: colonnes de blocking
        cutoff_by_col: seuils de pruning précoces par colonne (avant agrégation)
        right_batch_size: pour limiter RAM quand les blocs sont gros
        length_gate_ratio: si |lenL - lenR| / max(lenL,lenR) > ratio -> score annulé
    """

    def __init__(
        self,
        left_id: str,
        right_id: str,
        columns: List[str],
        peers: Dict[str, str],
        scorers: Dict[str, str],
        weights: Optional[Dict[str, float]] = None,
        global_min: float = 90.0,
        top_k: Optional[int] = None,
        block_by_left: Optional[str] = None,
        block_by_right: Optional[str] = None,
        cutoff_by_col: Optional[Dict[str, float]] = None,
        right_batch_size: int = 5000,
        length_gate_ratio: float = 0.6,
        workers: int = -1,
    ):
        self.left_id = left_id
        self.right_id = right_id
        self.columns = columns
        self.peers = peers
        self.scorers = {c: SCORER_REGISTRY[scorers[c]] for c in columns}
        self.weights = {c: (weights[c] if weights and c in weights else 1.0) for c in columns}
        self._w_arr = np.array([self.weights[c] for c in columns], dtype=float)
        self.global_min = float(global_min)
        self.top_k = top_k
        self.block_by_left = block_by_left
        self.block_by_right = block_by_right
        self.cutoff_by_col = cutoff_by_col or {}
        self.right_batch_size = max(1000, int(right_batch_size))
        self.length_gate_ratio = float(length_gate_ratio)
        self.workers = workers

        self._cols_triplets: List[Tuple[str, str, object]] = [
            (c, self.peers[c], self.scorers[c]) for c in self.columns
        ]

    # ---------- API ----------
    def match(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        need_cols_l = [self.left_id] + [c for c, _, _ in self._cols_triplets]
        need_cols_r = [self.right_id] + [rc for _, rc, _ in self._cols_triplets]

        left = (left.select(need_cols_l)
                    .with_columns([pl.col(c).cast(pl.Utf8, strict=False).fill_null("") for c in need_cols_l]))
        right = (right.select(need_cols_r)
                     .with_columns([pl.col(c).cast(pl.Utf8, strict=False).fill_null("") for c in need_cols_r]))

        results: List[pl.DataFrame] = []

        if self.block_by_left and self.block_by_right:
            lvals = set(left[self.block_by_left].unique().to_list())
            rvals = set(right[self.block_by_right].unique().to_list())
            for val in lvals.intersection(rvals):
                lblk = left.filter(pl.col(self.block_by_left) == val)
                rblk = right.filter(pl.col(self.block_by_right) == val)
                if lblk.height and rblk.height:
                    results.extend(self._match_block_batched(lblk, rblk))
        else:
            results.extend(self._match_block_batched(left, right))

        if not results:
            return pl.DataFrame(schema={self.left_id: pl.Utf8, self.right_id: pl.Utf8, "score": pl.Float64})

        out = pl.concat(results, how="vertical_relaxed")
        if self.top_k:
            out = _topk_per_left(out, self.top_k)
        return out

    # ---------- Internes ----------
    def _prep_clean_lists(self, df: pl.DataFrame, cols: List[str]) -> Dict[str, List[str]]:
        # map unique -> cleaned via cache
        cleaned: Dict[str, List[str]] = {}
        for c in cols:
            vals = df[c].to_list()
            cleaned[c] = [CompanyCleaner.clean(v) for v in vals]
        return cleaned

    def _length_gate_mask(self, L: List[str], R: List[str]) -> np.ndarray:
        la = np.array([len(x) for x in L], dtype=np.int32)[:, None]
        lb = np.array([len(x) for x in R], dtype=np.int32)[None, :]
        mx = np.maximum(la, lb).astype(np.float32)
        diff = np.abs(la - lb).astype(np.float32) / np.clip(mx, 1.0, None)
        # mask 1 si diff <= ratio sinon 0
        return (diff <= self.length_gate_ratio).astype(np.float32)

    def _match_block_batched(self, lblk: pl.DataFrame, rblk: pl.DataFrame) -> List[pl.DataFrame]:
        """Batches côté right pour contrôler la RAM."""
        results: List[pl.DataFrame] = []
        lcols = [c for c, _, _ in self._cols_triplets]
        rcols = [rc for _, rc, _ in self._cols_triplets]

        Lclean = self._prep_clean_lists(lblk, lcols)

        # batching right
        n = rblk.height
        if n == 0 or lblk.height == 0:
            return results

        for start in range(0, n, self.right_batch_size):
            end = min(start + self.right_batch_size, n)
            rchunk = rblk.slice(start, end - start)
            Rclean = self._prep_clean_lists(rchunk, rcols)

            mats: List[np.ndarray] = []
            for (lcol, rcol, scorer) in self._cols_triplets:
                cutoff = float(self.cutoff_by_col.get(lcol, 0.0))
                M = _cdist_col(Lclean[lcol], Rclean[rcol], scorer=scorer, cutoff=cutoff, workers=self.workers)
                mats.append(M)

            S = np.tensordot(self._w_arr, np.stack(mats, axis=0), axes=(0, 0)) / self._w_arr.sum()

            # Gate de longueur (annule scores trop divergents en taille)
            mask = self._length_gate_mask(Lclean[lcols[0]], Rclean[rcols[0]])
            # (on utilise la première colonne textuelle comme proxy; adapter au besoin)
            S = S * mask

            df_part = _matrix_to_df(S, lblk, rchunk, self.left_id, self.right_id, self.global_min)
            if df_part.height:
                results.append(df_part)

        return results
