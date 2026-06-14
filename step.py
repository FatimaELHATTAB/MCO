from typing import Iterator

import polars as pl

from matching_workflow import MatchingPipelineLazy


class ProviderRunner:
    def __init__(
        self,
        data_loader,
        cfg,
        provider: str,
        job_execution_id: int,
        logger,
    ):
        self.data_loader = data_loader
        self.cfg = cfg
        self.provider = provider
        self.job_execution_id = job_execution_id
        self.logger = logger

    def run_countries(self, provider: str | None = None):
        """
        Retourne les pays à traiter.
        """

        current_provider = provider or self.provider

        if current_provider in ["ORBIS", "REFINITIV"]:
            return [
                "MR", "RO", "SE", "AT", "ES", "FI", "LV", "BE", "LT", "MT",
                "CZ", "HU", "CY", "DK", "EE", "SK", "BG", "GR", "PT", "IE",
                "DE", "SI", "PL", "LU", "NL", "IT", "FR",
            ]

        self.logger.info(f"Retrieving countries for provider: {self.provider}")
        countries = self.data_loader.load_countries(self.job_execution_id)

        return countries

    def _to_lazy(
        self,
        df: pl.LazyFrame | pl.DataFrame,
    ) -> pl.LazyFrame:
        """
        Convertit en LazyFrame si besoin.
        """
        if isinstance(df, pl.DataFrame):
            return df.lazy()

        return df

    def _lazy_len(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
    ) -> int:
        """
        Calcule le nombre de lignes sans collecter toute la table.
        """
        if isinstance(lf, pl.DataFrame):
            return lf.height

        return lf.select(pl.len()).collect(streaming=True).item()

    def _iter_lazy_batches(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        batch_size: int,
    ) -> Iterator[tuple[int, pl.LazyFrame]]:
        """
        Découpe un LazyFrame/DataFrame en batchs.
        Retourne :
        - offset
        - batch LazyFrame
        """

        lf = self._to_lazy(lf)
        total_rows = self._lazy_len(lf)

        self.logger.info(f"Total rows to batch: {total_rows}")

        for offset in range(0, total_rows, batch_size):
            yield offset, lf.slice(offset, batch_size)

    def _explode_identity_once(
        self,
        df_identity: pl.LazyFrame | pl.DataFrame,
        identity_already_provided: bool,
    ) -> pl.LazyFrame:
        """
        Explose identity une seule fois.

        Si df_identity est fourni en entrée de run(), on considère qu'il peut déjà
        avoir été préparé par un pays précédent. Donc on évite de l'exploser deux fois.
        """

        df_identity = self._to_lazy(df_identity)

        if self.provider != "ORBIS":
            return df_identity

        if identity_already_provided:
            self.logger.info(
                "Identity was provided to run(); skipping identity explode to avoid double explode"
            )
            return df_identity

        self.logger.info("Exploding identity once before ORBIS batch matching")

        return self.data_loader.explode_df(df_identity)

    def _explode_orbis_batch(
        self,
        df_normalized_batch: pl.LazyFrame | pl.DataFrame,
    ) -> pl.LazyFrame:
        """
        Explose uniquement le batch ORBIS courant.
        """

        df_normalized_batch = self._to_lazy(df_normalized_batch)

        if self.provider != "ORBIS":
            return df_normalized_batch

        return self.data_loader.explode_df(df_normalized_batch)

    def _concat_dataframes(
        self,
        frames: list[pl.DataFrame],
    ) -> pl.DataFrame:
        """
        Concatène une liste de DataFrames.
        """

        if not frames:
            return pl.DataFrame()

        return pl.concat(
            frames,
            how="diagonal_relaxed",
        )

    def _deduplicate_matches(
        self,
        matches: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Union + déduplication finale.

        Pourquoi ?
        Comme on fait ORBIS batch par batch, le même couple peut ressortir plusieurs fois.
        On garde le meilleur match.
        """

        if matches.is_empty():
            return matches

        # D'abord on retire les doublons exacts.
        matches = matches.unique(maintain_order=True)

        # Si tu as un score, on garde le meilleur.
        if "confidence_score" in matches.columns:
            matches = matches.sort(
                "confidence_score",
                descending=True,
            )

        # Doublons métier : même local_id + même tpn_id
        subset_pair = [
            col for col in ["local_id", "tpn_id"]
            if col in matches.columns
        ]

        if len(subset_pair) == 2:
            matches = matches.unique(
                subset=subset_pair,
                keep="first",
                maintain_order=True,
            )

        # Si ton modèle métier impose un seul match par local_id,
        # on garde le meilleur local_id.
        if "local_id" in matches.columns:
            matches = matches.unique(
                subset=["local_id"],
                keep="first",
                maintain_order=True,
            )

        return matches

    def _remove_matched_from_not_matched(
        self,
        not_matched: pl.DataFrame,
        matches: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Nettoie le not_matched final.

        Un local_id peut apparaître comme not_matched dans un batch,
        puis matcher dans un autre résultat.
        Donc on retire du not_matched tous les local_id déjà présents dans matches.
        """

        if not_matched.is_empty():
            return not_matched

        if matches.is_empty():
            return not_matched

        if "local_id" not in not_matched.columns:
            return not_matched

        if "local_id" not in matches.columns:
            return not_matched

        matched_ids = matches.select("local_id").unique()

        not_matched = not_matched.join(
            matched_ids,
            on="local_id",
            how="anti",
        )

        return not_matched

    def run(
        self,
        country: str | None,
        df_identity: pl.LazyFrame | pl.DataFrame | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.LazyFrame]:
        """
        Lance le matching pour un pays.

        Stratégie ORBIS :
        - identity est chargée/explosée une seule fois
        - normalized / ORBIS est découpée en batchs
        - chaque batch ORBIS est explosé puis matché avec identity complète
        - les matches sont concaténés
        - déduplication finale
        """

        self.logger.info(
            f"Running matching for provider={self.provider} | country={country}"
        )

        rule_set = self.data_loader.get_matching_config(self.provider)

        identity_already_provided = df_identity is not None

        df_normalized, df_identity = self.data_loader.load(
            provider=self.provider,
            job_execution_id=self.job_execution_id,
            country=country,
            df_identity=df_identity,
        )

        df_normalized = self._to_lazy(df_normalized)
        df_identity = self._to_lazy(df_identity)

        self.logger.info("Data loaded successfully")

        # --------------------------------------------------
        # 1. Explode identity une seule fois
        # --------------------------------------------------
        df_identity = self._explode_identity_once(
            df_identity=df_identity,
            identity_already_provided=identity_already_provided,
        )

        # --------------------------------------------------
        # 2. Batch uniquement sur normalized / ORBIS
        # --------------------------------------------------
        batch_size = 2_000_000

        all_matches: list[pl.DataFrame] = []
        all_not_matched: list[pl.DataFrame] = []

        for batch_idx, (offset, df_normalized_batch) in enumerate(
            self._iter_lazy_batches(
                lf=df_normalized,
                batch_size=batch_size,
            ),
            start=1,
        ):
            self.logger.info(
                f"Starting batch {batch_idx} | offset={offset} | batch_size={batch_size}"
            )

            # --------------------------------------------------
            # 3. Explode du batch ORBIS courant
            # --------------------------------------------------
            df_normalized_batch = self._explode_orbis_batch(
                df_normalized_batch
            )

            pipeline = MatchingPipelineLazy(
                rule_set,
                left_id="local_id",
                right_id="tpn_id",
                run_fuzzy=False,
            )

            # --------------------------------------------------
            # 4. Matching batch ORBIS vs identity complète explosée
            # --------------------------------------------------
            matches, not_matched = pipeline.run(
                df_normalized_batch,
                df_identity,
            )

            if not matches.is_empty():
                matches = matches.with_columns(
                    pl.lit(batch_idx).alias("batch_id"),
                    pl.lit(offset).alias("batch_offset"),
                )

                all_matches.append(matches)

            if not not_matched.is_empty():
                not_matched = not_matched.with_columns(
                    pl.lit(batch_idx).alias("batch_id"),
                    pl.lit(offset).alias("batch_offset"),
                )

                all_not_matched.append(not_matched)

            self.logger.info(
                f"Batch {batch_idx} done | "
                f"matches={matches.shape} | "
                f"not_matched={not_matched.shape}"
            )

        # --------------------------------------------------
        # 5. Union des résultats
        # --------------------------------------------------
        matches = self._concat_dataframes(all_matches)
        not_matched = self._concat_dataframes(all_not_matched)

        self.logger.info(
            f"Before final cleanup | matches={matches.shape} | not_matched={not_matched.shape}"
        )

        # --------------------------------------------------
        # 6. Déduplication finale des matches
        # --------------------------------------------------
        matches = self._deduplicate_matches(matches)

        # --------------------------------------------------
        # 7. Nettoyage du not_matched
        # --------------------------------------------------
        not_matched = self._remove_matched_from_not_matched(
            not_matched=not_matched,
            matches=matches,
        )

        self.logger.info(
            f"Matching done | final matches={matches.shape} | final not_matched={not_matched.shape}"
        )

        return matches, not_matched, df_identity
