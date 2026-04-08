new_data = recalculate_clusters(df_formated)

cluster_stats_df = self.build_cluster_stats_from_df(new_data)

tpn.insert_df(new_data, table_key="matching")

if not cluster_stats_df.is_empty():
    tpn.insert_df(cluster_stats_df, table_key="cluster_stats")

remaining_data = self.filter_clustered_rows(new_data, cluster_stats_df)

self.pipeline_priorization(remaining_data)



----------

def filter_clustered_rows(self, new_data: pl.DataFrame, cluster_stats_df: pl.DataFrame) -> pl.DataFrame:
    """
    Retire de new_data les lignes appartenant aux clusters déjà sortis vers cluster_stats.
    """

    if new_data.is_empty() or cluster_stats_df.is_empty():
        return new_data

    clustered_ids = cluster_stats_df.select("cluster_id").unique()

    remaining_data = new_data.join(
        clustered_ids,
        on="cluster_id",
        how="anti"
    )

    return remaining_data


------------



import polars as pl


def build_cluster_stats_from_df(self, df: pl.DataFrame) -> pl.DataFrame:
    """
    Construit les cluster_stats directement depuis le dataframe du batch courant.
    """

    if df.is_empty():
        return pl.DataFrame()

    cluster_stats_df = (
        df.group_by(["cluster_id", "confidence_score"])
        .agg([
            pl.col("tpn_id").drop_nulls().unique().alias("legal_entity_ids"),
            pl.col("legal_entity").drop_nulls().unique().alias("legal_entity_names"),
            (
                pl.col("tpn_id").drop_nulls().n_unique() +
                pl.col("local_id").drop_nulls().n_unique()
            ).alias("cluster_size"),
            pl.col("dedup_decision").is_null().sum().alias("matching_to_process"),
            pl.len().alias("total_matching"),
            pl.col("flux_source").drop_nulls().unique().alias("sources"),
            pl.col("created_at").max().alias("created_at"),
            pl.col("updated_at").max().alias("updated_at"),
        ])
        .filter(pl.col("total_matching") > 2)
    )

    return cluster_stats_df
