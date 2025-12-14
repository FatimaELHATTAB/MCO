def _df_company_identifiers(self, df: pl.DataFrame) -> pl.DataFrame:
    mapping = self.conf["tables"]["company_identifier"]["derived_mapping"]
    dfs = []

    for m in mapping:
        src_col = m["source_col"]

        if src_col not in df.columns:
            continue

        # =========================
        # CASE 1: National Registry (JSON)
        # =========================
        if src_col == "national_registry_id":
            tmp = (
                df
                .filter(pl.col(src_col).is_not_null())
                .select([
                    pl.col("tpn_id").cast(pl.Utf8),
                    pl.col("flux_source").alias("comp_id_source").cast(pl.Utf8),
                    pl.col(src_col)
                ])
                .with_columns(
                    pl.col(src_col)
                    .map_elements(lambda x: list(x.items()) if x else [])
                    .alias("kv")
                )
                .explode("kv")
                .with_columns([
                    pl.col("kv").list.get(0).alias("comp_id_name").cast(pl.Utf8),
                    pl.col("kv").list.get(1).alias("comp_id").cast(pl.Utf8),
                    pl.lit("national_registry").alias("comp_id_nat").cast(pl.Utf8),
                ])
                .select([
                    "tpn_id",
                    "comp_id",
                    "comp_id_nat",
                    "comp_id_name",
                    "comp_id_source"
                ])
            )

            dfs.append(tmp)
            continue

        # =========================
        # CASE 2: Other identifiers (simple value)
        # =========================
        tmp = (
            df
            .select([
                pl.col("tpn_id").cast(pl.Utf8),
                pl.col(src_col).alias("comp_id").cast(pl.Utf8),
                pl.lit(m["comp_id_nat"]).alias("comp_id_nat").cast(pl.Utf8),
                pl.lit(m["comp_id_name"]).alias("comp_id_name").cast(pl.Utf8),
                pl.col("flux_source").alias("comp_id_source").cast(pl.Utf8),
            ])
            .filter(pl.col("comp_id").is_not_null())
        )

        dfs.append(tmp)

    return pl.concat(dfs) if dfs else pl.DataFrame(
        schema={
            "tpn_id": pl.Utf8,
            "comp_id": pl.Utf8,
            "comp_id_nat": pl.Utf8,
            "comp_id_name": pl.Utf8,
            "comp_id_source": pl.Utf8,
        }
    )
