from sqlalchemy import create_engine
import pandas as pd
import hashlib
import time
from sqlalchemy import text
import sys
sys.stdout.reconfigure(encoding='utf-8')
import os


# --------------------------
# 1️⃣ Paramètres PostgreSQL
# --------------------------
user = 'myuser'
password = 'strong_password'
host = 'localhost'
port = '5432'
database = 'dwh_pollution'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# --------------------------
# 2️⃣ Fonction de nettoyage
# --------------------------
def clean_dataset(file_path):
    # Charger la feuille spécifique
    df = pd.read_excel(file_path, sheet_name='Update 2024 (V6.1)')

    # Supprimer colonnes inutiles
    cols_to_drop = ['reference', 'web_link', 'who_ms', 'population_source']
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # Renommer colonnes
    df.rename(columns={
        'who_region': 'region',
        'country_name': 'country',
        'pm10_concentration': 'concentration_pm10',
        'pm25_concentration': 'concentration_pm25',
        'no2_concentration': 'concentration_no2',
        'pm10_tempcov': 'pm10_temp_cov',
        'pm25_tempcov': 'pm25_temp_cov',
        'no2_tempcov': 'no2_temp_cov',
        'type_of_stations': 'station_type'
    }, inplace=True)

    cols_pollutants = ['concentration_pm10', 'concentration_pm25', 'concentration_no2']
    
    cols_cov_pollutants = ['pm10_temp_cov', 'pm25_temp_cov', 'no2_temp_cov']

    # Imputation locale par city
    for col in cols_pollutants + cols_cov_pollutants:
        df[col] = df.groupby(['city'])[col].transform(lambda x: x.fillna(x.mean()))

    # Fallback par pays si NaN
    for col in cols_pollutants + cols_cov_pollutants:
        df[col] = df.groupby(['country'])[col].transform(lambda x: x.fillna(x.mean()))

    # Population manquante
    if 'population' in df.columns:
        df['population'] = df.groupby('country')['population'].transform(lambda x: x.fillna(x.mean()))

    # Convertir latitude, longitude en float
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    # Colonnes texte
    cols_to_str = ['region', 'iso3', 'country', 'city']
    df[cols_to_str] = df[cols_to_str].astype(str)

    # Année
    df['year'] = df['year'].fillna(0).astype(int)

    # Supprimer doublons
    df = df.drop_duplicates(subset=['country', 'city', 'year'])

    return df

# --------------------------
# 3️⃣ Fonction hash pour détecter changements
# --------------------------
def compute_hash(df):
    df_bytes = df.to_csv(index=False).encode()
    return hashlib.md5(df_bytes).hexdigest()

# --------------------------
# 4️⃣ Fonctions UPSERT pour PostgreSQL
# --------------------------
def add_timestamp_column():
    """Ajoute une colonne timestamp si elle n'existe pas"""
    sql = """
    ALTER TABLE etl.staging_d1 
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("  Colonne updated_at vérifiée/ajoutée")
def upsert_to_postgres(df, table_name='staging_d1'):
    """UPSERT : met à jour si existe, insère si nouveau"""
    
    # 1. Créer une table temporaire
    temp_table = f"temp_{table_name}"
    df.to_sql(temp_table, engine, 
              schema='etl', 
              if_exists='replace', 
              index=False)
    
    # 2. UPSERT avec ON CONFLICT - TOUTES les colonnes
    upsert_sql = f"""
    INSERT INTO etl.{table_name} (
        region, iso3, country, city, year,
        concentration_pm10, concentration_pm25, concentration_no2,
        pm10_temp_cov, pm25_temp_cov, no2_temp_cov,
        station_type, population, latitude, longitude, updated_at
    )
    SELECT
        region::text,
        iso3::text,
        country::text,
        city::text,
        year::int,
        concentration_pm10::double precision,
        concentration_pm25::double precision,
        concentration_no2::double precision,
        pm10_temp_cov::double precision,
        pm25_temp_cov::double precision,
        no2_temp_cov::double precision,
        station_type::text,
        population::double precision,
        latitude::double precision,
        longitude::double precision,
        CURRENT_TIMESTAMP
    FROM etl.{temp_table}
    ON CONFLICT (country, city, year) DO UPDATE SET
        region = EXCLUDED.region,
        iso3 = EXCLUDED.iso3,
        concentration_pm10 = EXCLUDED.concentration_pm10,
        concentration_pm25 = EXCLUDED.concentration_pm25,
        concentration_no2 = EXCLUDED.concentration_no2,
        pm10_temp_cov = EXCLUDED.pm10_temp_cov,
        pm25_temp_cov = EXCLUDED.pm25_temp_cov,
        no2_temp_cov = EXCLUDED.no2_temp_cov,
        station_type = EXCLUDED.station_type,
        population = EXCLUDED.population,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        updated_at = CURRENT_TIMESTAMP;
    """

    
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        # Supprimer la table temporaire
        conn.execute(text(f"DROP TABLE etl.{temp_table}"))
    
    print(f" UPSERT effectué pour {table_name}")
def load_to_postgres(df):
    """Initial load - à utiliser seulement pour la première fois"""
    # D'abord créer la table avec updated_at
    add_timestamp_column()
    
    # Puis faire un UPSERT qui agira comme un INSERT complet
    upsert_to_postgres(df)
    print("  Données chargées initialement dans PostgreSQL")
def append_to_postgres(new_df):
    """Pour compatibilité avec l'ancien code - utilise maintenant UPSERT"""
    print("  append_to_postgres() est déprécié, utilisation de UPSERT à la place")
    upsert_to_postgres(new_df)
def delete_rows_safe(city_name):
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM etl.staging_d1 WHERE TRIM(LOWER(city)) = TRIM(LOWER(:city))"),
            {"city": city_name}
        )
    print(f"  Lignes de la ville {city_name} supprimées")

# Données de test (gardées pour compatibilité)
new_data = {
    'region': ['Europe'],
    'iso3': ['FRA'],
    'country': ['France'],
    'city': ['X'],
    'year': [2024],
    'concentration_pm10': [25.0],
    'concentration_pm25': [15.0],
    'concentration_no2': [30.0],
    'pm10_temp_cov': [0.1],
    'pm25_temp_cov': [0.05],
    'no2_temp_cov': [0.07],
    'station_type': ['Urban'],
    'population': [2148000],
    'latitude': [48.8566],
    'longitude': [2.3522]
}

new_df = pd.DataFrame(new_data)
# 5️⃣ Pipeline automatique
# --------------------------
file_path = r"data\who_ambient_air_quality_database_version_2024_(v6.1).xlsx"

# Créer dossier hash
hash_dir = "hash"
os.makedirs(hash_dir, exist_ok=True)
hash_file = os.path.join(hash_dir, "hash_d1.txt")

# Vérifier hash précédent
if os.path.exists(hash_file):
    with open(hash_file, "r") as f:
        previous_hash = f.read().strip()
else:
    previous_hash = None

print(" Vérification des changements...")

df = clean_dataset(file_path)
output_dir = "staging_csv"
os.makedirs(output_dir, exist_ok=True)  # créer le dossier si nécessaire

csv_file = os.path.join(output_dir, "staging_d1.csv")
df.to_csv(csv_file, index=False, encoding='utf-8')
print(f" Données du staging sauvegardées dans {csv_file}")

new_hash = compute_hash(df)

if new_hash != previous_hash:
    print(" Modifications détectées → Mise à jour PostgreSQL...")
    
    # 1. Ajouter la colonne timestamp si nécessaire
    add_timestamp_column()
    # 2. Faire l'UPSERT avec les données principales
    upsert_to_postgres(df)
    
    # 3. UPSERT des données de test (comme avant)
    #upsert_to_postgres(new_df)    

    
    # 4. Supprimer les données de test (comme avant)
    #delete_rows_safe("X")

    # 5. Sauvegarder nouveau hash
    with open(hash_file, "w") as f:
        f.write(new_hash)
    
    print(" Mise à jour UPSERT terminée avec succès")
    
else:
    print(" Aucune modification trouvée, rien à faire.")




