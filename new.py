from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime, time
from typing import Any

import polars as pl


class DbClient:
    def read_as_polars(
        self,
        query: str,
        schema: dict[str, pl.DataType] | None = None,
        params: tuple | dict | None = None,
    ) -> pl.DataFrame:
        """
        Exécute une requête SQL et retourne un DataFrame Polars robuste.

        Comportement:
        - schema=None:
            tente une inférence Polars après normalisation des valeurs
            puis fallback en Utf8 si une colonne contient des types mixtes
        - schema fourni:
            applique le schéma explicitement

        Hypothèse:
        - self._conn() retourne une connexion DB-API 2.0
        """
        conn = self._conn()

        try:
            with conn.cursor() as cur:
                if params is not None:
                    cur.execute(query, params)
                else:
                    cur.execute(query)

                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []

            if not columns:
                return pl.DataFrame()

            if not rows:
                if schema is not None:
                    return pl.DataFrame(schema=schema)
                return pl.DataFrame(schema={col: pl.Null for col in columns})

            normalized_rows = [self._normalize_row(row, len(columns)) for row in rows]

            # Cas 1: schéma explicite
            if schema is not None:
                data = {col: [] for col in columns}
                for row in normalized_rows:
                    for col, value in zip(columns, row):
                        data[col].append(value)

                return pl.DataFrame(data, schema=schema, strict=False)

            # Cas 2: pas de schéma -> tentative d'inférence directe
            try:
                data = {col: [] for col in columns}
                for row in normalized_rows:
                    for col, value in zip(columns, row):
                        data[col].append(value)

                return pl.DataFrame(data, strict=False)

            # Cas 3: fallback robuste si types mixtes
            except Exception:
                safe_data = {}
                for idx, col in enumerate(columns):
                    col_values = [row[idx] for row in normalized_rows]
                    safe_data[col] = self._coerce_mixed_column(col_values)

                return pl.DataFrame(safe_data, strict=False)

        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture SQL vers Polars: {e}") from e

    def _normalize_row(self, row: Any, expected_len: int) -> list[Any]:
        """
        Transforme une row SQL en liste de valeurs propres pour Polars.
        """
        if row is None:
            return [None] * expected_len

        values = list(row)
        if len(values) != expected_len:
            raise ValueError(
                f"Row invalide: longueur {len(values)} != nombre de colonnes {expected_len}"
            )

        return [self._normalize_value(v) for v in values]

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalise les types exotiques provenant du driver SQL.
        """
        if value is None:
            return None

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, (datetime, date, time, str, int, float, bool)):
            return value

        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return value.hex()

        if isinstance(value, dict):
            # On garde une représentation stable au lieu de prendre arbitrairement la 1re valeur
            return str(value)

        if isinstance(value, (list, tuple, set)):
            return str(list(value))

        # UUID, enums, objets driver DB, etc.
        return str(value)

    def _coerce_mixed_column(self, values: list[Any]) -> list[Any]:
        """
        Si une colonne contient des types mixtes incompatibles,
        convertit proprement en chaîne tout en gardant None.
        """
        non_null_types = {type(v) for v in values if v is not None}

        if len(non_null_types) <= 1:
            return values

        # Tolérance int/float/bool -> on peut convertir vers float ou garder tel quel.
        numeric_types = {int, float, bool}
        if non_null_types.issubset(numeric_types):
            return [None if v is None else float(v) for v in values]

        return [None if v is None else str(v) for v in values]
