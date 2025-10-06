import hashlib
import polars as pl
import pytest

# ⬇️ adapte "normalizer" si besoin
from normalizer import (
    recalculate_clusters,
    _build_graph,
    _find_clusters,
    _map_clusters,
    hash_cluster,
    get_matching_table_schema,
)


# ---------- Fixtures ----------

@pytest.fixture
def edges_df():
    # Graphe attendu (non expiré):  A-B-C  |  D-E  |  F seul
    # On met un lien expiré C-D pour vérifier fic=True (doit l'ignorer)
    return pl.DataFrame({
        "intern_id":   ["A", "B", "C", "D", "E", "F", "C"],
        "target_id":   ["B", "C", "A", "E", "D", "F", "D"],   # C-D est dupliqué ici (ligne 6) mais expiré
        "intern_name": ["ia","ib","ic","id","ie","if","ic"],
        "target_name": ["T1","T1","T1","T2","T2","T3","T2"],
        "matching_rule": ["r"]*7,
        "matching_start_date": [None]*7,
        "cluster_id": [None]*7,
        "matching_end_date": [None, None, None, None, None, None, "2024-01-01"],  # <- expire C-D
        "closure_reason": [None]*7,
        "is_duplicate": [False]*7,
        "lisbon_decision": [False]*7,
        "lisbon_date": [None]*7,
        "lisbon_user": [None]*7,
        "confidence_score": [None]*7,
        "country": ["FR"]*7,
    })


# ---------- _build_graph ----------

def test_build_graph_symmetry(edges_df):
    graph = _build_graph(edges_df)
    # Toutes les arêtes doivent être non orientées
    for row in edges_df.iter_rows(named=True):
        s, t = row["intern_id"], row["target_id"]
        assert t in graph.get(s, set())
        assert s in graph.get(t, set())


# ---------- _find_clusters ----------

def test_find_clusters_from_graph(edges_df):
    # Construire un graphe sur les lignes NON expirées (comme fic=True)
    alive = edges_df.filter(pl.col("matching_end_date").is_null())
    graph = _build_graph(alive)
    clusters = _find_clusters(graph)

    # Convertir en ensembles triés pour comparaison robuste
    norm = sorted([tuple(sorted(c)) for c in clusters])
    assert norm == sorted([("A", "B", "C"), ("D", "E"), ("F",)])


# ---------- hash_cluster ----------

def test_hash_cluster_deterministic():
    # ordre des noeuds ne doit pas changer le hash
    h1 = hash_cluster(["B", "A", "C"], "T1")
    h2 = hash_cluster(["C", "B", "A"], "T1")
    assert h1 == h2

    # target_name doit impacter le hash
    h3 = hash_cluster(["A", "B", "C"], "T9")
    assert h3 != h1

    # contrôler la forme (hexdigest 64 chars)
    assert isinstance(h1, str) and len(h1) == 64
    # et que c'est bien sha256 de la concat triée
    combined = "--".join(sorted(set(["A", "B", "C"]))) + "--T1"
    assert h1 == hashlib.sha256(combined.encode()).hexdigest()


# ---------- _map_clusters ----------

def test_map_clusters_produces_hash_per_node(edges_df):
    alive = edges_df.filter(pl.col("matching_end_date").is_null())
    graph = _build_graph(alive)

    mapping = _map_clusters(graph, edges_df)

    # Tous les noeuds présents dans le graphe doivent être mappés
    expected_nodes = set(pl.concat([
        alive["intern_id"], alive["target_id"]
    ]).unique().to_list())
    assert expected_nodes.issubset(set(mapping.keys()))

    # Tous les noeuds d'un même cluster doivent partager le même hash
    h_abc = {mapping["A"], mapping["B"], mapping["C"]}
    h_de = {mapping["D"], mapping["E"]}
    h_f = {mapping["F"]}
    assert len(h_abc) == 1
    assert len(h_de) == 1
    assert len(h_f) == 1
    assert not ({*h_abc} & {*h_de} & {*h_f})  # hashes différents entre clusters

    # Le hash attendu utilise target_name unique du cluster
    # cluster ABC -> target_name "T1"
    expected_hash_abc = hash_cluster(["A", "B", "C"], "T1")
    assert mapping["A"] == expected_hash_abc


# ---------- recalculate_clusters (intégration) ----------

def test_recalculate_clusters_fic_true(edges_df):
    # fic=True : ignore la ligne expirée (C-D), donc clusters: ABC, DE, F
    out = recalculate_clusters(edges_df, fic=True)
    assert "cluster_id" in out.columns

    # Récupérer les ids attendus en recomputant comme la fonction le fait
    alive = edges_df.filter(pl.col("matching_end_date").is_null())
    expected_mapping = _map_clusters(_build_graph(alive), edges_df)

    # Chaque ligne porte le cluster du intern_id correspondant
    for intern_id, cluster_id in out.select(["intern_id", "cluster_id"]).iter_rows():
        assert cluster_id == expected_mapping[intern_id]


def test_recalculate_clusters_fic_false(edges_df):
    # fic=False : inclut aussi l’arête expirée C-D dans le graphe
    out = recalculate_clusters(edges_df, fic=False)
    assert "cluster_id" in out.columns

    full_graph = _build_graph(edges_df)  # inclut C-D
    expected_mapping = _map_clusters(full_graph, edges_df)

    # Le mapping doit coller au graph complet
    for intern_id, cluster_id in out.select(["intern_id", "cluster_id"]).iter_rows():
        assert cluster_id == expected_mapping[intern_id]

    # Avec l’arête C-D, tout le monde est connecté -> un seul hash pour A..F
    assert len(set(out["cluster_id"].unique().to_list())) == 1


# ---------- get_matching_table_schema ----------

def test_get_matching_table_schema_shape_and_dtypes():
    schema = get_matching_table_schema()
    # clés minimales attendues (d'après la capture)
    expected_keys = {
        "intern_id", "target_id", "intern_name", "target_name",
        "matching_rule", "matching_start_date", "cluster_id",
        "matching_end_date", "closure_reason", "is_duplicate",
        "lisbon_decision", "lisbon_date", "lisbon_user",
        "confidence_score", "country"
    }
    assert expected_keys.issubset(set(schema.keys()))

    # quelques dtypes critiques
    assert schema["intern_id"] == pl.Utf8
    assert schema["target_id"] == pl.Utf8
    assert schema["is_duplicate"] == pl.Boolean
    assert schema["lisbon_decision"] == pl.Boolean
    assert schema["matching_start_date"] == pl.Date
    assert schema["matching_end_date"] == pl.Date
