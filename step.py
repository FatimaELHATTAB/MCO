# helpers.py

def extract_tpn_ids(df, column: str = "tpn_id") -> list:
    if df is None or len(df) == 0:
        return []

    if hasattr(df, "to_pandas"):
        df = df.to_pandas()

    return (
        df[column]
        .dropna()
        .drop_duplicates()
        .tolist()
    )


---


from historization_repository import HistorizationRepository
from helpers import extract_tpn_ids


def process_immatriculation_update(self, df_update):
    """
    UPDATE_IDENTIFICATION

    -> historisation
    -> puis réutilisation du process existant
    """

    tpn_ids = extract_tpn_ids(df_update)

    historizer = HistorizationRepository(
        db=self.db,
        schema=self.schema,
    )

    historizer.historize_by_tpn_ids(
        tpn_ids=tpn_ids,
        source_action="UPDATE_IDENTIFICATION",
        close_tables=[
            "address",
            "company_identifier",
            "tax",
            "isin_equity",
            "relationship_role_with_entity",
        ],
        archive_legal_entity=True,
        delete_legal_entity=False,
    )

    # réutilisation du process existant
    self.process_immatriculation(df_update)



from historization_repository import HistorizationRepository
from helpers import extract_tpn_ids


def prepare_update_matching(self, df_update):
    """
    UPDATE_MATCHING

    -> neutralisation ancienne base
    -> matching normal ensuite
    """

    tpn_ids = extract_tpn_ids(df_update)

    historizer = HistorizationRepository(
        db=self.db,
        schema=self.schema,
    )

    historizer.historize_by_tpn_ids(
        tpn_ids=tpn_ids,
        source_action="UPDATE_MATCHING",
        close_tables=[
            "matching",
            "cluster_stats",
            "address",
            "company_identifier",
            "tax",
            "isin_equity",
            "relationship_role_with_entity",
        ],
        archive_legal_entity=True,
        delete_legal_entity=False,
    )
