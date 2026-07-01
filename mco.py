import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_cprofile_output(filepath, top_n=10):
    """
    Analyse un fichier texte issu de cProfile et affiche un barplot des fonctions les plus consommatrices en temps.

    Args:
        filepath (str): Chemin vers le fichier texte contenant les résultats de profiling.
        top_n (int): Nombre de fonctions à afficher dans le graphique (par défaut 10).
    """
    parsed_rows = []
    with open(filepath, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    headers = ['ncalls', 'tottime', 'percall', 'cumtime', 'cum_percall', 'function']

    for line in lines[1:]:
        parts = line.strip().split()
        if len(parts) < 6:
            continue
        numeric_parts = parts[:5]
        function_name = " ".join(parts[5:])
        parsed_rows.append(numeric_parts + [function_name])

    df = pd.DataFrame(parsed_rows, columns=headers)

    for col in ['ncalls', 'tottime', 'percall', 'cumtime', 'cum_percall']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df_sorted = df.sort_values(by='tottime', ascending=False).head(top_n)

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_sorted, y="function", x="tottime")
    plt.xlabel("Total Time (tottime)")
    plt.title(f"Top {top_n} Fonctions les plus consommatrices en temps")
    plt.tight_layout()
    plt.gca().invert_yaxis()
    plt.grid(True)
    plt.show()

data_text = """
ncalls  tottime  percall  cumtime  percall  filename:lineno(function)
8       1365.309 170.664  365.309 170.664  matching_table.py:245(process_matching)
7       1073.450 134.181  173.450 134.181  matching_table.py:261(recalculate_clusters)
617503  941.867  0.002    941.867  0.002    fuzzy_script
617503  941.867  0.002    541.867  0.002    write_to_db
617503  941.867  0.002    692.328  0.002    read_from_db
"""

# Reformat and reparse the data correctly: first 5 columns are numbers, the rest is a string
lines = data_text.strip().split('\n')
parsed_rows = []
for line in lines[1:]:
    parts = line.strip().split()
    if len(parts) >= 6:
        numeric_parts = parts[:5]
        function_name = " ".join(parts[5:])
        parsed_rows.append(numeric_parts + [function_name])

# Create DataFrame with proper columns
df = pd.DataFrame(parsed_rows, columns=[
    'ncalls', 'tottime', 'percall', 'cumtime', 'cum_percall', 'function'
])

# Convert numeric columns
for col in ['ncalls', 'tottime', 'percall', 'cumtime', 'cum_percall']:
    df[col] = pd.to_numeric(df[col])

# Sort by tottime descending
df_sorted = df.sort_values(by='cumtime', ascending=False).head(10)

# Plot
plt.figure(figsize=(12, 6))
sns.barplot(data=df_sorted, y="function", x="cumtime")
plt.xlabel("Total Time (tottime)")
plt.title("Top 10 Fonctions les plus consommatrices en temps (tottime)")
plt.tight_layout()
plt.gca().invert_yaxis()
plt.grid(True)
plt.show()


# Sort by cumtime descending
df_sorted = df.sort_values(by='cumtime', ascending=False).head(10)

# Plot Pie Chart
plt.figure(figsize=(10, 8))
plt.pie(
    df_sorted['cumtime'],
    labels=df_sorted['function'],
    autopct='%1.1f%%',
    startangle=140
)
plt.title("Répartition des 5 fonctions les plus consommatrices (cumtime)")
plt.axis('equal')  # Pour que le cercle soit bien rond
plt.tight_layout()
plt.show()


import matplotlib.pyplot as plt

# Données
labels = ['conn.execute', 'polars.read_database']
times = [692, 319]  # en millisecondes

# Création du graphique
plt.figure(figsize=(6, 4))
bars = plt.bar(labels, times)
plt.ylabel("Temps de lecture (ms)")
plt.title("Comparaison des temps de lecture")

# Ajout des valeurs au-dessus des barres
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 6, f'{yval} s', ha='center', va='bottom')

plt.tight_layout()
plt.show()

