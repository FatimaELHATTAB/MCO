import polars as pl

def ensure_tpn_id(self, df: pl.DataFrame) -> pl.DataFrame:
    """
    Return mapping (flux_source, local_id) -> tpn_id
    - RCX/RMPM: tpn_id = local_id
    - Others: tpn_id generated via get_tpn_id(), one per unique key
    """
    keys = (
        df.select(["flux_source", "local_id"])
          .unique()
    )

    # 1) keys where tpn_id = local_id
    fixed = (
        keys.filter(pl.col("flux_source").is_in(["RMPM", "RCX"]))
            .with_columns(pl.col("local_id").cast(pl.Utf8).alias("tpn_id"))
            .select(["flux_source", "local_id", "tpn_id"])
    )

    # 2) keys where we must generate a new tpn_id
    to_gen = keys.filter(~pl.col("flux_source").is_in(["RMPM", "RCX"]))

    if to_gen.height > 0:
        # generate exactly one id per unique key
        gen_ids = [str(get_tpn_id()) for _ in range(to_gen.height)]
        generated = (
            to_gen.with_columns(pl.Series("tpn_id", gen_ids))
                  .select(["flux_source", "local_id", "tpn_id"])
        )
        mapping = pl.concat([fixed, generated], how="vertical")
    else:
        mapping = fixed

    return mapping



def get_normalized(self, table_full_name: str, mapping: pl.DataFrame) -> pl.DataFrame:
    """
    Load from <table_full_name> filtered by (flux_source, local_id) in mapping
    """
    # construire une liste SQL (flux_source, local_id)
    keys = mapping.select(["flux_source", "local_id"]).to_dicts()
    if not keys:
        return pl.DataFrame()

    # ⚠️ si énorme volume, on chunk. Ici version simple.
    values = ",".join([f"('{k['flux_source']}', '{k['local_id']}')" for k in keys])

    query = f"""
        SELECT *
        FROM {table_full_name}
        WHERE (flux_source, local_id) IN ({values})
          AND closed_at IS NULL
    """

    return self.db.read_as_polars(query=query, schema=None)


def attach_tpn_id(self, df_norm: pl.DataFrame, mapping: pl.DataFrame) -> pl.DataFrame:
    return df_norm.join(mapping, on=["flux_source", "local_id"], how="inner")


def process_matching(self, df_not_matched: pl.LazyFrame | pl.DataFrame):
    df_in = df_not_matched.collect() if hasattr(df_not_matched, "collect") else df_not_matched

    # 1) mapping (flux_source, local_id) -> tpn_id
    mapping = self.ensure_tpn_id(df_in)

    # 2) ADDRESS
    addr_norm = self.get_normalized(f"{self.schema}.address_normalized", mapping)
    addr_norm = self.attach_tpn_id(addr_norm, mapping)
    df_addr = self._df_for_table("address", addr_norm)  # YAML columns must include tpn_id
    self._insert_df(df_addr, "address")

    # 3) COMPANY IDENTIFIER
    comp_norm = self.get_normalized(f"{self.schema}.company_identifier_normalized", mapping)
    comp_norm = self.attach_tpn_id(comp_norm, mapping)
    df_comp = self._df_for_table("company_identifier", comp_norm)
    self._insert_df(df_comp, "company_identifier")

    # 4) LEGAL ENTITY
    le_norm = self.get_normalized(f"{self.schema}.legal_entity_normalized", mapping)
    le_norm = self.attach_tpn_id(le_norm, mapping)
    df_le = self._df_for_table("legal_entity", le_norm)
    self._insert_df(df_le, "legal_entity")

    self.logger.info("TPN pipeline: DONE ✓")
