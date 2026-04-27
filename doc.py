@dataclass(frozen=True)
class MatchingRule:
    level: str
    name: str
    rmpm_cols: List[str]
    provider_cols: List[str]
    kind: str
    cardinality: int
    confidence_level: int

    scorers: Optional[Dict[str, str]] = None
    weights: Optional[Dict[str, float]] = None
    global_min: Optional[float] = None
    block_by_left: Optional[List[str]] = None
    block_by_right: Optional[List[str]] = None
    top_k: Optional[int] = None

--------
scorers=cfg.get("scorers"),
weights=cfg.get("weights"),
global_min=cfg.get("global_min"),
block_by_left=cfg.get("block_by_left"),
block_by_right=cfg.get("block_by_right"),
top_k=cfg.get("top_k"),



-------





def _init_fuzzy_from_rules(self, rule) -> Optional[RapidFuzzyMatcher]:
    peers = dict(zip(rule.rmpm_cols, rule.provider_cols))

    return RapidFuzzyMatcher(
        left_id=self.left_id,
        right_id=self.right_id,
        columns=rule.rmpm_cols,
        peers=peers,
        scorers=rule.scorers or {c: "jw" for c in rule.rmpm_cols},
        weights=rule.weights or {c: 1.0 for c in rule.rmpm_cols},
        global_min=rule.global_min or rule.confidence_level,
        block_by_left=rule.block_by_left,
        block_by_right=rule.block_by_right,
        top_k=rule.top_k,
    )



--------


need_cols_l = [self.left_id] + self.columns
need_cols_r = [self.right_id] + [self.peers[c] for c in self.columns]

if self.block_by_left:
    need_cols_l += self.block_by_left

if self.block_by_right:
    need_cols_r += self.block_by_right

need_cols_l = list(dict.fromkeys(need_cols_l))
need_cols_r = list(dict.fromkeys(need_cols_r))







-----------

def _block_key(cols):
    return pl.concat_str(
        [pl.col(c).cast(pl.Utf8).fill_null("") for c in cols],
        separator="|",
    )




------




if self.block_by_left and self.block_by_right:
    left = left.with_columns(_block_key(self.block_by_left).alias("__block_key"))
    right = right.with_columns(_block_key(self.block_by_right).alias("__block_key"))

    lvals = set(left["__block_key"].unique().to_list())
    rvals = set(right["__block_key"].unique().to_list())

    for val in lvals & rvals:
        lblk = left.filter(pl.col("__block_key") == val)
        rblk = right.filter(pl.col("__block_key") == val)
        results.extend(self._match_block_batched(lblk, rblk))
else:
    results.extend(self._match_block_batched(left, right))
