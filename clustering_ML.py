import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import os

# ------------------------ Dossier de sauvegarde ------------------------
output_dir = "output_visuals"
os.makedirs(output_dir, exist_ok=True)

# ------------------------ Connexion à la BD ------------------------
engine = create_engine(
    "postgresql://myuser:strong_password@localhost:5432/dwh_pollution"
)
# ------------------------ Fonctions Utilitaires ------------------------
def display_cluster_info(df, features, cluster_col, level_name, cluster_names):
    print(f"\n===== Informations sur les clusters ({level_name}) =====")
    cluster_summary = df.groupby(cluster_col)[features].mean()
    cluster_sizes = df.groupby(cluster_col).size()
    cluster_summary.index = [cluster_names[i] for i in cluster_summary.index]
    cluster_sizes.index = [cluster_names[i] for i in cluster_sizes.index]
    cluster_intervals = df.groupby(cluster_col)[features].agg(['min','max'])
    cluster_intervals.index = [cluster_names[i] for i in cluster_intervals.index]
    print("Taille des clusters:\n", cluster_sizes)
    print("\nProfil moyen des clusters (valeurs originales):\n", cluster_summary)
    print("\nIntervalle de valeurs par cluster:\n", cluster_intervals)
    return cluster_summary, cluster_sizes, cluster_intervals

def format_for_display(df, features):
    df_display = df.copy()
    for f in features:
        if 'co2' in f or 'methane' in f or 'nitrous' in f:
            df_display[f] = df_display[f].round(2).astype(str) + ' Mt'
        elif 'pm' in f or 'no2' in f:
            df_display[f] = df_display[f].round(2).astype(str) + ' µg/m³'
    return df_display

def plot_cluster_heatmap(cluster_summary, features, title, filename):
    plt.figure(figsize=(12,5))
    annotated_df = format_for_display(cluster_summary, features)
    sns.heatmap(cluster_summary, annot=annotated_df, cmap="RdYlBu_r", fmt="")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename))
    plt.show()

def save_plotly_figure(fig, filename, save_png=False):
    fig.write_html(os.path.join(output_dir, filename+".html"))
    if save_png:
        fig.write_image(os.path.join(output_dir, filename+".png"))

# ------------------------ Sélection automatique de k ------------------------
def find_optimal_k(X, max_k=6):
    best_k = 2
    best_score = -1
    silhouettes = []
    for k in range(2, max_k+1):
        km = KMeans(n_clusters=k, random_state=42)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        silhouettes.append(score)
        if score > best_score:
            best_score = score
            best_k = k
    # Plot silhouette
    plt.figure()
    plt.plot(range(2, max_k+1), silhouettes, marker='o')
    plt.xlabel("Nombre de clusters k")
    plt.ylabel("Silhouette Score")
    plt.title("Sélection du k optimal (Silhouette)")
    plt.grid(True)
    plt.savefig(os.path.join(output_dir,"silhouette_k.png"))
    plt.show()
    return best_k

# ------------------------ Clustering Villes ------------------------
query_city_pollution = """
WITH pollution_avg AS (
    SELECT 
        l.location_id, l.country, l.city,
        AVG(f.pm10) AS pm10, AVG(f.pm25) AS pm25, AVG(f.no2) AS no2,
        l.latitude, l.longitude
    FROM dw.fact_air_quality f
    JOIN dw.dim_location l ON f.location_id = l.location_id
    GROUP BY l.location_id, l.country, l.city, l.latitude, l.longitude
)
SELECT country, city, pm10, pm25, no2, latitude, longitude
FROM pollution_avg
"""
df_city_poll = pd.read_sql(query_city_pollution, engine).fillna(0)
features_poll = ['pm10','pm25','no2']
X_poll = StandardScaler().fit_transform(df_city_poll[features_poll])

# Déterminer k automatiquement
k_city = find_optimal_k(X_poll, max_k=6)
print("Nombre optimal de clusters pour les villes :", k_city)

# Clustering KMeans
kmeans_poll = KMeans(n_clusters=k_city, random_state=42)
df_city_poll['cluster_pollution'] = kmeans_poll.fit_predict(X_poll)

# Attribution noms significatifs
cluster_order = df_city_poll.groupby('cluster_pollution')['pm10'].mean().sort_values().index.tolist()
cluster_names_poll = {cluster_order[i]: f'Cluster {i+1}' for i in range(len(cluster_order))}
df_city_poll['cluster_pollution_name'] = df_city_poll['cluster_pollution'].map(cluster_names_poll)

# Affichage info & heatmap
cluster_summary_poll, cluster_sizes_poll, cluster_intervals_poll = display_cluster_info(
    df_city_poll, features_poll, 'cluster_pollution', 'Villes - Pollution (KMeans)', cluster_names_poll
)
plot_cluster_heatmap(cluster_summary_poll, features_poll, "Profil moyen des clusters (villes - pollution, KMeans)", "heatmap_villes_kmeans.png")

# Carte interactive villes
fig_map_villes = px.scatter_mapbox(
    df_city_poll,
    lat='latitude',
    lon='longitude',
    color='cluster_pollution_name',
    hover_name='city',
    hover_data={f:True for f in features_poll},
    zoom=1,
    height=600,
    color_discrete_sequence=px.colors.qualitative.Plotly
)
fig_map_villes.update_layout(mapbox_style="open-street-map", title="Clusters Villes - Pollution (KMeans)")
save_plotly_figure(fig_map_villes, "map_villes_kmeans",save_png=True)
fig_map_villes.show()

# Scatter 3D villes
fig_3d_villes = px.scatter_3d(
    df_city_poll,
    x='pm10',
    y='pm25',
    z='no2',
    color='cluster_pollution_name',
    hover_name='city',
    hover_data={'country':True, 'pm10':True, 'pm25':True, 'no2':True},
    title="Scatter 3D Villes - Pollution (KMeans)",
    color_discrete_sequence=px.colors.qualitative.Plotly,
    height=700
)
fig_3d_villes.update_layout(scene=dict(
    xaxis_title='PM10 (µg/m³)',
    yaxis_title='PM2.5 (µg/m³)',
    zaxis_title='NO2 (µg/m³)'
))
save_plotly_figure(fig_3d_villes, "scatter3D_villes_kmeans")
fig_3d_villes.show()

# ------------------------ Clustering Pays ------------------------
query_country_pollution = """
WITH emissions_sum AS (
    SELECT 
        l.country,
        SUM(f.co2) AS co2,
        SUM(f.methane) AS methane,
        SUM(f.nitrous_oxide) AS nitrous
    FROM dw.fact_emissions f
    JOIN dw.dim_location l ON f.location_id = l.location_id
    GROUP BY l.country
)
SELECT country, co2, methane, nitrous
FROM emissions_sum
"""
df_country_poll = pd.read_sql(query_country_pollution, engine).fillna(0)
features_c_poll = ['co2','methane','nitrous']

# Transformation log pour normalisation
df_country_poll_processed = df_country_poll.copy()
df_country_poll_processed[features_c_poll] = np.log1p(df_country_poll[features_c_poll])
X_c_poll_log = StandardScaler().fit_transform(df_country_poll_processed[features_c_poll])

# Déterminer k automatiquement
k_country = find_optimal_k(X_c_poll_log, max_k=6)
print("Nombre optimal de clusters pour les pays :", k_country)

# Clustering KMeans
kmeans_c_poll = KMeans(n_clusters=k_country, random_state=42)
df_country_poll['cluster_pollution'] = kmeans_c_poll.fit_predict(X_c_poll_log)

# Attribution noms significatifs
cluster_order_c = df_country_poll.groupby('cluster_pollution')['co2'].mean().sort_values().index.tolist()
cluster_names_c_poll = {cluster_order_c[i]: f'Cluster {i+1}' for i in range(len(cluster_order_c))}
df_country_poll['cluster_pollution_name'] = df_country_poll['cluster_pollution'].map(cluster_names_c_poll)

# Heatmap
cluster_summary_c_poll, cluster_sizes_c_poll, cluster_intervals_c_poll = display_cluster_info(
    df_country_poll, features_c_poll, 'cluster_pollution', 'Pays - Pollution (KMeans)', cluster_names_c_poll
)
plot_cluster_heatmap(cluster_summary_c_poll, features_c_poll, "Profil moyen des clusters (pays - pollution, KMeans)", "heatmap_pays_kmeans.png")

# Choropleth interactive pays
geojson_url = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json'
df_country_poll_hover = df_country_poll.copy()
for f in features_c_poll:
    df_country_poll_hover[f+'_display'] = df_country_poll_hover[f].round(2).astype(str) + ' Mt'
hover_data_country = {f+'_display':True for f in features_c_poll}

fig_country = px.choropleth(
    df_country_poll_hover,
    geojson=geojson_url,
    locations='country',
    featureidkey="properties.name",
    color='cluster_pollution_name',
    hover_name='country',
    hover_data=hover_data_country,
    color_discrete_map={f'Cluster {i+1}': c for i,c in enumerate(['green','orange','red','purple','blue','brown'])},
    title="Clusters Pays - Profil Pollution (KMeans)"
)
fig_country.update_geos(fitbounds="locations", visible=False)
save_plotly_figure(fig_country, "map_pays_kmeans", save_png=True)
fig_country.show()

# Scatter 3D pays
fig_3d_pays = px.scatter_3d(
    df_country_poll,
    x='co2',
    y='methane',
    z='nitrous',
    color='cluster_pollution_name',
    hover_name='country',
    hover_data={f:True for f in ['co2','methane','nitrous']},
    title="Scatter 3D Pays - Pollution (KMeans)",
    color_discrete_map={f'Cluster {i+1}': c for i,c in enumerate(['green','orange','red','purple','blue','brown'])},
    height=700
)
fig_3d_pays.update_layout(scene=dict(
    xaxis_title='CO2 (Mt)',
    yaxis_title='CH4 (Mt)',
    zaxis_title='N2O (Mt)'
))
save_plotly_figure(fig_3d_pays, "scatter3D_pays_kmeans")
fig_3d_pays.show()

print("\n✅ Clustering pollution terminé avec KMeans et nombre de clusters automatisé.")
