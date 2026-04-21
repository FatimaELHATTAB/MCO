# tpn_insertion.py

class TpnInsertion:

    def process_immatriculation_update(self, df_updates):

        def iter_batches(df, batch_size=20000):
            for start in range(0, len(df), batch_size):
                yield df[start:start + batch_size]

        df_c = df_updates.collect() if hasattr(df_updates, "collect") else df_updates

        repo = VersioningRepository(self.db, self.schema)

        for batch in iter_batches(df_c):

            # 🔑 récupérer les tpn_id existants
            mapping = repo.resolve_existing_tpn_id(batch)

            # ---------- LEGAL ENTITY ----------
            le_norm = self.get_normalized(
                table_full_name=f"{self.schema}.legal_entity_normalized",
                mapping=mapping
            )
            le_norm = self.attach_tpn_id(le_norm, mapping)

            df_le = self._df_for_table("legal_entity", le_norm)

            repo.version_legal_entity(df_le)

            # ---------- ADDRESS ----------
            addr_norm = self.get_normalized(
                table_full_name=f"{self.schema}.address_normalized",
                mapping=mapping
            )

            if addr_norm.height != 0:
                addr_norm = self.attach_tpn_id(addr_norm, mapping)
                df_addr = self._df_for_table("address", addr_norm)

                repo.close_and_insert("address", df_addr)

            # ---------- COMPANY IDENTIFIER ----------
            comp_norm = self.get_normalized(
                table_full_name=f"{self.schema}.company_identifier_normalized",
                mapping=mapping
            )
            comp_norm = self.attach_tpn_id(comp_norm, mapping)
            df_comp = self._df_for_table("company_identifier", comp_norm)

            repo.close_and_insert("company_identifier", df_comp)

            # ---------- TAX ----------
            tax_norm = self.get_normalized(
                table_full_name=f"{self.schema}.tax_normalized",
                mapping=mapping
            )
            tax_norm = self.attach_tpn_id(tax_norm, mapping)
            df_tax = self._df_for_table("tax", tax_norm)

            repo.close_and_insert("tax", df_tax)



-----------







