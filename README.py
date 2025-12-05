schema: own_91109_svg_um

tables:

  identification:
    name: t_tpn_identification
    columns:
      - tpn_id
      - comp_name
      - incrp_count
      - d_incorp
      - n_emp
      - leg_form
      - d_lei_cert
      - d_immat_lei
      - s_lei
      - vat_num
      - sta_vat_num
      - loc_act_code_type
      - loc_act_code
      - nace_code
      - tp_rel_own
      - business_cntry
      - legal_status
      - reg_req
      - data_share_stat
      - data_source
      - cert_status
      - cert_owner
      - val_dat
      - csr_flag
      - c_typo

  address:
    name: t_tpn_address
    columns:
      - tpn_id
      - address_type
      - address_distribution
      - address_way
      - address_hamlet
      - address_postcode_city
      - address_country
      - dept_iso
      - subdept_iso
      - building_name_iso
      - floor_iso
      - room_iso
      - street_name_iso
      - building_number_iso
      - post_box_iso
      - town_location_name_iso
      - post_code_iso
      - town_name_iso
      - country_iso
      - district_name_iso
      - country_subdivision_iso

  company_identifier:
    name: t_tpn_company_identifier
    derived_mapping:
      - source_col: national_registry_id
        comp_id_nat: national_registry
        comp_id_name: National registry
      - source_col: international_registry_id
        comp_id_nat: international_registry
        comp_id_name: International registry
      - source_col: market_data_id
        comp_id_nat: market_data
        comp_id_name: Market data
      - source_col: bnpp_business_is_id
        comp_id_nat: bnpp_business_is
        comp_id_name: BNPP Business ID




import polars as pl
import yaml
from src.core.priorisation.immatriculation import get_tpn_id

class TpnInsertion:
    def __init__(self, db_client, logger, config_path="conf/tpn_insertion_config.yaml"):
        self.db = db_client
        self.logger = logger

        with open(config_path, "r") as f:
            self.conf = yaml.safe_load(f)

        self.schema = self.conf["schema"]

    # ------------------------------------------------------------ #
    # Generate TPN ID if missing
    # ------------------------------------------------------------ #
    def ensure_tpn_id(self, df):
        if "tpn_id" not in df.columns:
            df = df.with_columns(
                pl.element().map_elements(lambda _: get_tpn_id()).alias("tpn_id")
            )
        return df

    # ------------------------------------------------------------ #
    # Extract dataframe for a specific table based on YAML mapping
    # ------------------------------------------------------------ #
    def _df_for_table(self, table_key: str, df: pl.DataFrame):
        cols = self.conf["tables"][table_key].get("columns", [])
        if not cols:
            return pl.DataFrame()
        return df.select([c for c in cols if c in df.columns])

    # ------------------------------------------------------------ #
    # Build Company Identifier DF from normalized columns
    # ------------------------------------------------------------ #
    def _df_company_identifiers(self, df: pl.DataFrame):
        mapping = self.conf["tables"]["company_identifier"]["derived_mapping"]
        dfs = []

        for m in mapping:
            src_col = m["source_col"]
            if src_col not in df.columns:
                continue

            tmp = df.select([
                "tpn_id",
                pl.col(src_col).alias("comp_id"),
                pl.lit(m["comp_id_nat"]).alias("comp_id_nat"),
                pl.lit(m["comp_id_name"]).alias("comp_id_name"),
                pl.lit("normalized_source").alias("comp_id_source")
            ]).filter(pl.col("comp_id").is_not_null())

            dfs.append(tmp)

        return pl.concat(dfs) if dfs else pl.DataFrame()

    # ------------------------------------------------------------ #
    # Insert using generic SQL INSERT (db_client.insert_records)
    # ------------------------------------------------------------ #
    def _insert_df(self, df: pl.DataFrame, table_key: str):
        if df.is_empty():
            return

        table_name = self.conf["tables"][table_key]["name"]
        full_name = f"{self.schema}.{table_name}"

        cols = df.columns
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)

        query = f"""
        INSERT INTO {full_name} ({col_list})
        VALUES ({placeholders})
        """

        self.db.insert_records(query, df.to_pandas().values.tolist())
        self.logger.info(f"Inserted {df.height} rows into {full_name}")

    # ------------------------------------------------------------ #
    # MAIN PIPELINE CALLED BY PROCESS MATCHING
    # ------------------------------------------------------------ #
    def process_matching(self, df):
        df = self.ensure_tpn_id(df)

        df_ident = self._df_for_table("identification", df)
        df_addr = self._df_for_table("address", df)
        df_comp = self._df_company_identifiers(df)

        self._insert_df(df_ident, "identification")
        self._insert_df(df_addr, "address")
        self._insert_df(df_comp, "company_identifier")

        self.logger.info("TPN Insertion: DONE ✔")
