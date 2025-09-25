def read_as_polars(self, query: str, schema_overrides: dict | None = None) -> pl.DataFrame:
    conn = self._conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]  # ordre exact du SELECT

        # construis le DF avec les bons noms
        df = pl.DataFrame(rows, schema=cols)  # (ou columns=cols sur versions plus anciennes)

        # cast optionnel (uniquement sur les colonnes présentes)
        if schema_overrides:
            df = df.with_columns([
                pl.col(name).cast(dtype, strict=False)
                for name, dtype in schema_overrides.items()
                if name in df.columns
            ])
        return df
    finally:
        self._put(conn)
