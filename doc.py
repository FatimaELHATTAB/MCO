    def resolve_existing_tpn_id(self, df):
        temp = f"tmp_resolve_{uuid.uuid4().hex[:6]}"
        self.db.create_temp_table_from_df(temp, df)

        sql = f"""
        SELECT src.*, le.tpn_id
        FROM {temp} src
        LEFT JOIN {self.schema}.legal_entity le
          ON le.local_id = src.local_id
         AND le.flux_source = src.flux_source
         AND le.closed_at IS NULL
        """

        result = self.db.select_df(sql)
        self.db.execute(f"DROP TABLE {temp}")
        return result


def close_and_insert(self, table_name, df):

        temp = f"tmp_{table_name}_{uuid.uuid4().hex[:6]}"
        self.db.create_temp_table_from_df(temp, df)

        # CLOSE
        self.db.execute(f"""
        UPDATE {self.schema}.{table_name} t
        SET closed_at = NOW(),
            business_validity_to = NOW()
        FROM {temp} src
        WHERE t.tpn_id = src.tpn_id
          AND t.closed_at IS NULL
        """)

        # INSERT
        self.db.execute(f"""
        INSERT INTO {self.schema}.{table_name}
        SELECT *, NOW() as created_at, NOW() as business_validity_from
        FROM {temp}
        """)

        self.db.execute(f"DROP TABLE {temp}")
