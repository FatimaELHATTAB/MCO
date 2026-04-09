import polars as pl


def _to_pg_array_literal(values: list[str] | None) -> str | None:
    if values is None:
        return None

    escaped = []
    for v in values:
        if v is None:
            continue
        s = str(v)
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        escaped.append(f'"{s}"')

    return "{" + ",".join(escaped) + "}"

def prepare_df_for_postgres_copy(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    result = df

    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.List):
            result = result.with_columns(
                pl.col(col)
                .map_elements(_to_pg_array_literal, return_dtype=pl.Utf8)
                .alias(col)
            )

    return result
