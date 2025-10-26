# rapidfuzzy_matcher.py
from __future__ import annotations

import unicodedata
from typing import List, Dict, Optional, Tuple
import numpy as np
import polars as pl
from rapidfuzz import process, fuzz, distance


# --- Scorers disponibles (référencés par nom dans ton YAML) ---
SCORER_REGISTRY = {
    "WRatio": fuzz.WRatio,
    "QRatio": fuzz.QRatio,
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "jaro_winkler": distance.JaroWinkler.normalized_similarity,
    "lev": distance.Levenshtein.normalized_similarity,
    # ajoute ici ceux dont tu as besoin
}


# ---------- Utils vectorisés ----------
def _norm(s: Optional[str]) -> str:
    """Normalisation légère (one-shot) : trim, lower, déaccentuation."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _cdist_col(
    left_vals: List[str],
    right_vals: List[str],
    scorer,
    cutoff: float = 0.0,
    workers: int = -1,
) -> np.ndarray:
    """
    Matrice |left| x |right| des similarités. Pruning via score_cutoff.
    NB: fuzz.* -> [0..100], distance.* normalized_similarity -> [0..1].
    """
    M = process.cdist(left_vals, right_vals, scorer=scorer, score_cutoff=cutoff, workers=workers)
    # Harmonise en [0..100] si nécessaire
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
    """Top-k par left_id sans tri global (rank fenêtré)."""
    if df.height == 0:
        return df
    return (
        df.with_columns(
            pl.col("score").rank(method="dense", descending=True).over("left_id").alias("rk")
        )
        .filter(pl.col("rk") <= k)
        .drop("rk")
    )


# ---------- Classe principale ----------
class RapidFuzzyMatcher:
    """
    Fuzzy matching vectorisé avec RapidFuzz + Polars.

    Paramètres clés :
      - columns: liste des colonnes côté left à comparer.
      - peers: dict left_col -> right_col (colonnes homologues).
      - scorers: dict col -> nom de scorer (cf. SCORER_REGISTRY).
      - weights: dict col -> poids (float); 1.0 par défaut.
      - global_min: seuil de score final (0..100).
      - top_k: garder k meilleurs matches par left_id (None = tous).
      - block_by_left / block_by_right: colonnes de blocking.
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
    ):
        self.left_id = left_id
        self.right_id = right_id
        self.columns = columns
        self.peers = peers

        # Compile les scorers une fois
        self.scorers = {c: SCORER_REGISTRY[scorers[c]] for c in columns}

        # Poids en array pour tensordot
        self.weights = {c: (weights[c] if weights and c in weights else 1.0) for c in columns}
        self._w_arr = np.array([self.weights[c] for c in columns], dtype=float)

        self.global_min = float(global_min)
        self.top_k = top_k
        self.block_by_left = block_by_left
        self.block_by_right = block_by_right

        # Triplets (left_col, right_col, scorer) dans un ordre stable
        self._cols_triplets: List[Tuple[str, str, object]] = [
            (c, self.peers[c], self.scorers[c]) for c in self.columns
        ]

    # -------- API publique --------
    def match(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        """Retourne un DataFrame Polars: [left_id, right_id, score]."""

        # Cast + fill une seule fois (évite erreurs de type & None)
        need_cols_l = [self.left_id] + [c for c, _, _ in self._cols_triplets]
        need_cols_r = [self.right_id] + [rc for _, rc, _ in self._cols_triplets]

        left = (
            left.select(need_cols_l)
            .with_columns([pl.col(c).cast(pl.Utf8, strict=False).fill_null("") for c in need_cols_l])
        )
        right = (
            right.select(need_cols_r)
            .with_columns([pl.col(c).cast(pl.Utf8, strict=False).fill_null("") for c in need_cols_r])
        )

        results: List[pl.DataFrame] = []

        # Blocking (intersection native Polars)
        if self.block_by_left and self.block_by_right:
            lvals = left.select(pl.col(self.block_by_left).unique()).to_series()
            rvals = right.select(pl.col(self.block_by_right).unique()).to_series()
            block_values = lvals.intersect(rvals)

            for val in block_values:
                lblk = left.filter(pl.col(self.block_by_left) == val)
                rblk = right.filter(pl.col(self.block_by_right) == val)
                if lblk.height and rblk.height:
                    results.append(self._match_block_vectorized(lblk, rblk))
        else:
            results.append(self._match_block_vectorized(left, right))

        if not results:
            return pl.DataFrame(schema={self.left_id: pl.Utf8, self.right_id: pl.Utf8, "score": pl.Float64})

        out = pl.concat(results, how="vertical_relaxed")
        if self.top_k:
            out = _topk_per_left(out, self.top_k)
        return out

    # -------- Détails internes --------
    def _match_block_vectorized(self, lblk: pl.DataFrame, rblk: pl.DataFrame) -> pl.DataFrame:
        """Calcule les similarités par colonne (cdist), agrège pondéré, puis filtre par seuil global."""
        if lblk.height == 0 or rblk.height == 0:
            return pl.DataFrame({self.left_id: [], self.right_id: [], "score": []})

        # Normalisation en listes Python (one-shot)
        left_norm_cols: List[List[str]] = []
        right_norm_cols: List[List[str]] = []
        for lcol, rcol, _ in self._cols_triplets:
            left_norm_cols.append([_norm(x) for x in lblk[lcol].to_list()])
            right_norm_cols.append([_norm(x) for x in rblk[rcol].to_list()])

        # Matrices de similarité par colonne
        mats: List[np.ndarray] = []
        for (lvals, rvals), (_, _, scorer) in zip(zip(left_norm_cols, right_norm_cols), self._cols_triplets):
            M = _cdist_col(lvals, rvals, scorer=scorer, cutoff=0.0, workers=-1)  # pruning fin par global_min
            mats.append(M)

        if not mats:
            return pl.DataFrame({self.left_id: [], self.right_id: [], "score": []})

        # Agrégation pondérée (shape: |L| x |R|)
        S = np.tensordot(self._w_arr, np.stack(mats, axis=0), axes=(0, 0)) / self._w_arr.sum()

        # Filtrage par seuil global
        return _matrix_to_df(S, lblk, rblk, self.left_id, self.right_id, self.global_min)


# --------------- Exemple d'utilisation ---------------
if __name__ == "__main__":
    # Petit exemple synthétique ; à supprimer dans ta prod.
    left = pl.DataFrame(
        {
            "left_id": ["A", "B", "C"],
            "legal_name": ["Total Energies SE", "BNP Paribas", "Renault Group"],
            "country": ["FR", "FR", "FR"],
        }
    )
    right = pl.DataFrame(
        {
            "right_id": ["r1", "r2", "r3"],
            "lb_raisn_scial_cleaned": ["TotalEnergies", "BNP Paribas SA", "Groupe Renault"],
            "incorp_country": ["FR", "FR", "FR"],
        }
    )

    matcher = RapidFuzzyMatcher(
        left_id="left_id",
        right_id="right_id",
        columns=["legal_name", "country"],
        peers={"legal_name": "lb_raisn_scial_cleaned", "country": "incorp_country"},
        scorers={"legal_name": "token_sort_ratio", "country": "ratio"},
        weights={"legal_name": 0.8, "country": 0.2},
        global_min=70.0,
        top_k=2,
        block_by_left="country",
        block_by_right="incorp_country",
    )

    out = matcher.match(left, right)
    print(out)
