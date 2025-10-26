# rapidfuzzy_matcher.py
from __future__ import annotations

from typing import Dict, List, Optional, Callable
import numpy as np
import polars as pl
from rapidfuzz import fuzz, process, distance


# ------------------------- Scorers (RF 2.15.1) -------------------------
def _jw_100(a: str, b: str, **kwargs) -> float:
    """
    Jaro-Winkler normalisé sur 0..100.
    **kwargs permet d'ignorer processor / score_cutoff envoyés par cdist().
    """
    return 100.0 * distance.JaroWinkler.normalized_similarity(a, b)


SCORER_REGISTRY: Dict[str, Callable[[str, str], float]] = {
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort": fuzz.token_sort_ratio,
    "token_set": fuzz.token_set_ratio,
    "jw": _jw_100,
}


# ------------------------------ Utils ----------------------------------
def _prep_strings(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """
    Normalisation légère: cast en string, null -> "", trim, lower.
    (Une seule fois avant le scoring)
    """
    exprs = []
    for c in cols:
        if c in df.columns:
            exprs.append(
                pl.col(c)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .str.to_lowercase()
                .alias(c)
            )
    return df.with_columns(exprs) if exprs else df


def _cdist(
    left_vals: List[str],
    right_vals: List[str],
    scorer: Callable[[str, str], float],
    cutoff: float,
) -> np.ndarray:
    """
    Matrice de similarité (float32) en [0..100], prunée par score_cutoff.
    Utilise tous les cœurs (workers=-1).
    """
    M = process.cdist(
        left_vals,
        right_vals,
        scorer=scorer,
        score_cutoff=cutoff,
        workers=-1,
    )
    # s'assure d'un dtype léger
    return M.astype(np.float32, copy=False)


# --------------------------- Classe principale --------------------------
class RapidFuzzyMatcher:
    """
    Fuzzy matching vectorisé avec RapidFuzz (2.15.1) + Polars.

    Params
    ------
    left_id / right_id : noms des colonnes identifiants
    columns            : colonnes à scorer côté left
    peers              : mapping left_col -> right_col
    scorers            : mapping col -> clé dans SCORER_REGISTRY
    weights            : mapping col -> poids (float). Default = 1.0
    global_min         : cutoff global (0..100). Paires < cutoff ignorées.
    block_by_left      : colonne de blocking côté left (optionnel)
    block_by_right     : colonne de blocking côté right (optionnel)
    top_k              : conserver les k meilleurs candidats par left_id (optionnel)
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
        block_by_left: Optional[str] = None,
        block_by_right: Optional[str] = None,
        top_k: Optional[int] = None,
    ):
        self.left_id = left_id
        self.right_id = right_id
        self.columns = columns
        self.peers = peers or {}
        self.scores = scorers or {}
        self.weights = weights or {c: 1.0 for c in columns}
        self.global_min = float(global_min)
        self.block_by_left = block_by_left
        self.block_by_right = block_by_right
        self.top_k = top_k

        # validation des scorers
        for c in self.columns:
            key = self.scores.get(c)
            if key not in SCORER_REGISTRY:
                raise ValueError(
                    f"Unknown scorer for column '{c}': {key!r}. "
                    f"Valid: {list(SCORER_REGISTRY.keys())}"
                )

    # ----------------------------- API publique -----------------------------

    def match(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        """Retourne un DataFrame Polars: left_id, right_id, score (float)."""
        if left.is_empty() or right.is_empty():
            return self._empty_out()

        # Colonnes à préparer (scoring + éventuels blocks)
        left_cols = self.columns + ([self.block_by_left] if self.block_by_left else [])
        right_cols = [self.peers.get(c, c) for c in self.columns] + (
            [self.block_by_right] if self.block_by_right else []
        )

        left_prep = _prep_strings(left, [c for c in left_cols if c in left.columns])
        right_prep = _prep_strings(right, [c for c in right_cols if c in right.columns])

        if self.block_by_left and self.block_by_right:
            # valeurs communes des blocs
            lvals = (
                left_prep.select(pl.col(self.block_by_left).drop_nulls().unique())
                .to_series()
                .to_list()
            )
            rvals_set = set(
                right_prep.select(pl.col(self.block_by_right).drop_nulls().unique())
                .to_series()
                .to_list()
            )
            common = [v for v in lvals if v in rvals_set]

            results = []
            for v in common:
                lf = left_prep.filter(pl.col(self.block_by_left) == v)
                rf = right_prep.filter(pl.col(self.block_by_right) == v)
                if lf.height and rf.height:
                    results.append(self._match_block(lf, rf))
            out = pl.concat(results) if results else self._empty_out()
        else:
            # pas de blocking -> 1 passe
            out = self._match_block(left_prep, right_prep)

        if self.top_k:
            out = (
                out.sort(by=["left_id", "score"], descending=[False, True])
                .group_by("left_id")
                .head(self.top_k)
            )
        return out

    # --------------------------- Implémentation ----------------------------

    def _match_block(self, left_blk: pl.DataFrame, right_blk: pl.DataFrame) -> pl.DataFrame:
        """Match vectorisé sur un bloc (ou sur tout le dataset s'il n'y a pas de bloc)."""
        l_ids = left_blk[self.left_id].to_list()
        r_ids = right_blk[self.right_id].to_list()
        if not l_ids or not r_ids:
            return self._empty_out()

        rcols = [self.peers.get(c, c) for c in self.columns]
        scorer_funcs = [SCORER_REGISTRY[self.scores[c]] for c in self.columns]
        weights = np.array([float(self.weights.get(c, 1.0)) for c in self.columns], dtype=np.float32)
        wsum = float(weights.sum()) if float(weights.sum()) > 0 else 1.0

        # valeurs par colonne (une seule fois)
        L = {c: left_blk[c].to_list() for c in self.columns}
        R = {c: right_blk[rc].to_list() for c, rc in zip(self.columns, rcols)}

        # matrices de scores par colonne
        mats: List[np.ndarray] = []
        for c, scorer, w in zip(self.columns, scorer_funcs, weights):
            if w == 0:
                continue
            M = _cdist(L[c], R[c], scorer=scorer, cutoff=self.global_min)
            if M.size == 0:
                # si tout a été pruné par cutoff, on crée une matrice nulle de la bonne shape
                M = np.zeros((len(L[c]), len(R[c])), dtype=np.float32)
            mats.append(M * w)

        if not mats:
            return self._empty_out()

        # combinaison pondérée -> score final
        S = np.add.reduce(mats) / wsum  # shape: (n_left, n_right)

        # ne garder que >= cutoff
        mask = S >= self.global_min
        if not mask.any():
            return self._empty_out()

        li = np.repeat(np.asarray(l_ids, dtype=object), S.shape[1]).reshape(S.shape)
        ri = np.tile(np.asarray(r_ids, dtype=object), (S.shape[0], 1))

        out = pl.DataFrame(
            {
                "left_id": li[mask].ravel().tolist(),
                "right_id": ri[mask].ravel().tolist(),
                "score": S[mask].ravel().astype(np.float32).tolist(),
            },
            schema={"left_id": pl.Utf8, "right_id": pl.Utf8, "score": pl.Float64},
        )
        return out

    @staticmethod
    def _empty_out() -> pl.DataFrame:
        return pl.DataFrame(schema={"left_id": pl.Utf8, "right_id": pl.Utf8, "score": pl.Float64})


