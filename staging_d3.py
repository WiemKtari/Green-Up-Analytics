from sqlalchemy import create_engine, text
import pandas as pd
import hashlib
import time
import numpy as np
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

def clean_dataset_d3(file_path_d3):
    df2 = pd.read_csv(file_path_d3)

    # Garder seulement après 1980
    df2 = df2[df2['year'] > 1980]

    # Colonnes polluants
    cols_polluant_emission = [
        'co2', 'cement_co2','coal_co2','consumption_co2','flaring_co2','gas_co2',
        'oil_co2','other_industry_co2','methane','nitrous_oxide'
    ]

    # 1️⃣ Imputation moyenne pays/année puis pays
    for col in cols_polluant_emission:
        df2[col] = df2.groupby(['country','year'])[col].transform(lambda x: x.fillna(x.mean()))
        df2[col] = df2.groupby(['country'])[col].transform(lambda x: x.fillna(x.mean()))

    # 2️⃣ Remplissage cohérent des polluants quand il manque UNE seule colonne
    cols_for_balance = [
        'co2','cement_co2','coal_co2','flaring_co2',
        'gas_co2','oil_co2','other_industry_co2'
    ]

    def fill_missing_pollutant(row):
        values = row[cols_for_balance]
        missing = values[values.isna()].index

        if len(missing) == 1:
            miss = missing[0]
            others = values.drop(miss)

            if miss == 'co2':
                return miss, others.sum()
            else:
                return miss, row['co2'] - others[others.index != 'co2'].sum()

        return None, None

    for idx, row in df2.iterrows():
        missing_col, missing_val = fill_missing_pollutant(row)
        if missing_col:
            df2.at[idx, missing_col] = missing_val

    # 3️⃣ Imputer GDP & population
    if 'gdp' in df2.columns:
        df2['gdp'] = df2.groupby('country')['gdp'].transform(lambda x: x.fillna(x.mean()))

    df2['population'] = df2.groupby('country')['population'].transform(lambda x: x.fillna(x.mean()))

    # 4️⃣ Colonnes per capita
    cols_per_capita = [
        'co2_per_capita', 'cement_co2_per_capita','coal_co2_per_capita','consumption_co2_per_capita',
        'flaring_co2_per_capita','gas_co2_per_capita','oil_co2_per_capita',
        'other_co2_per_capita','methane_per_capita','nitrous_oxide_per_capita'
    ]

    for col in cols_per_capita:
        base = col.replace('_per_capita', '')
        if base in df2.columns:
            df2[col] = df2[col].fillna(df2[base] / df2['population'])

    # 5️⃣ Supprimer lignes où tout CO₂ est NaN
    core = ['co2','cement_co2','coal_co2','consumption_co2','flaring_co2','gas_co2',
            'oil_co2','other_industry_co2']

    df2 = df2[~df2[core].isna().all(axis=1)]

    # 6️⃣ Pourcentages
    sources = ['cement_co2','coal_co2','flaring_co2','gas_co2','oil_co2','other_industry_co2']
    for src in sources:
        df2[f'{src}_pct'] = (df2[src] / df2['co2'].replace(0, np.nan)) * 100

    # 7️⃣ Colonnes finales
    cols_to_keep = [
        'cement_co2_pct','coal_co2_pct','flaring_co2_pct','gas_co2_pct','oil_co2_pct','other_industry_co2_pct',
        'cement_co2','coal_co2','consumption_co2','flaring_co2','gas_co2','oil_co2','other_industry_co2','co2',
        'co2_per_capita', 'cement_co2_per_capita','coal_co2_per_capita','consumption_co2_per_capita',
        'flaring_co2_per_capita','gas_co2_per_capita','oil_co2_per_capita','other_co2_per_capita',
        'methane_per_capita','nitrous_oxide_per_capita',
        'methane','nitrous_oxide','country','year','iso_code','population'
    ]

    df2 = df2[cols_to_keep]

    return df2

import pandas as pd

# --------------------------
# 3️⃣ Fonction hash pour détecter changements
# --------------------------
def compute_hash(df):
    df_bytes = df.to_csv(index=False).encode()
    return hashlib.md5(df_bytes).hexdigest()

# --------------------------
# 4️⃣ Fonctions UPSERT pour PostgreSQL
# --------------------------
def add_timestamp_column(table_name='staging_d3'):
    """Ajoute une colonne timestamp si elle n'existe pas"""
    sql = f"""
    ALTER TABLE etl.{table_name} 
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"  Colonne updated_at vérifiée/ajoutée pour {table_name}")

def upsert_to_postgres(df, table_name='staging_d3'):
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
        cement_co2_pct, coal_co2_pct, flaring_co2_pct, gas_co2_pct, oil_co2_pct, other_industry_co2_pct,
        cement_co2, coal_co2, consumption_co2, flaring_co2, gas_co2, oil_co2, other_industry_co2, co2,
        co2_per_capita, cement_co2_per_capita, coal_co2_per_capita, consumption_co2_per_capita,
        flaring_co2_per_capita, gas_co2_per_capita, oil_co2_per_capita, other_co2_per_capita,
        methane_per_capita, nitrous_oxide_per_capita,
        methane, nitrous_oxide, country, year, iso_code, population, updated_at
    )
    SELECT
        cement_co2_pct::double precision,
        coal_co2_pct::double precision,
        flaring_co2_pct::double precision,
        gas_co2_pct::double precision,
        oil_co2_pct::double precision,
        other_industry_co2_pct::double precision,
        cement_co2::double precision,
        coal_co2::double precision,
        consumption_co2::double precision,
        flaring_co2::double precision,
        gas_co2::double precision,
        oil_co2::double precision,
        other_industry_co2::double precision,
        co2::double precision,
        co2_per_capita::double precision,
        cement_co2_per_capita::double precision,
        coal_co2_per_capita::double precision,
        consumption_co2_per_capita::double precision,
        flaring_co2_per_capita::double precision,
        gas_co2_per_capita::double precision,
        oil_co2_per_capita::double precision,
        other_co2_per_capita::double precision,
        methane_per_capita::double precision,
        nitrous_oxide_per_capita::double precision,
        methane::double precision,
        nitrous_oxide::double precision,
        country::text,
        year::int,
        iso_code::text,
        population::double precision,
        CURRENT_TIMESTAMP
    FROM etl.{temp_table}
    ON CONFLICT (country, year) DO UPDATE SET
        cement_co2_pct = EXCLUDED.cement_co2_pct,
        coal_co2_pct = EXCLUDED.coal_co2_pct,
        flaring_co2_pct = EXCLUDED.flaring_co2_pct,
        gas_co2_pct = EXCLUDED.gas_co2_pct,
        oil_co2_pct = EXCLUDED.oil_co2_pct,
        other_industry_co2_pct = EXCLUDED.other_industry_co2_pct,
        cement_co2 = EXCLUDED.cement_co2,
        coal_co2 = EXCLUDED.coal_co2,
        consumption_co2 = EXCLUDED.consumption_co2,
        flaring_co2 = EXCLUDED.flaring_co2,
        gas_co2 = EXCLUDED.gas_co2,
        oil_co2 = EXCLUDED.oil_co2,
        other_industry_co2 = EXCLUDED.other_industry_co2,
        co2 = EXCLUDED.co2,
        co2_per_capita = EXCLUDED.co2_per_capita,
        cement_co2_per_capita = EXCLUDED.cement_co2_per_capita,
        coal_co2_per_capita = EXCLUDED.coal_co2_per_capita,
        consumption_co2_per_capita = EXCLUDED.consumption_co2_per_capita,
        flaring_co2_per_capita = EXCLUDED.flaring_co2_per_capita,
        gas_co2_per_capita = EXCLUDED.gas_co2_per_capita,
        oil_co2_per_capita = EXCLUDED.oil_co2_per_capita,
        other_co2_per_capita = EXCLUDED.other_co2_per_capita,
        methane_per_capita = EXCLUDED.methane_per_capita,
        nitrous_oxide_per_capita = EXCLUDED.nitrous_oxide_per_capita,
        methane = EXCLUDED.methane,
        nitrous_oxide = EXCLUDED.nitrous_oxide,
        iso_code = EXCLUDED.iso_code,
        population = EXCLUDED.population,
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
    add_timestamp_column('staging_d3')
    
    # Puis faire un UPSERT qui agira comme un INSERT complet
    upsert_to_postgres(df, 'staging_d3')
    print("  Données chargées initialement dans PostgreSQL")

def append_to_postgres(new_df):
    """Pour compatibilité avec l'ancien code - utilise maintenant UPSERT"""
    print("  append_to_postgres() est déprécié, utilisation de UPSERT à la place")
    upsert_to_postgres(new_df, 'staging_d3')

def delete_rows_safe(country_name):
    """Supprime les lignes d'un pays donné"""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM etl.staging_d3 WHERE TRIM(LOWER(country)) = TRIM(LOWER(:country))"),
            {"country": country_name}
        )
    print(f"  Lignes du pays {country_name} supprimées")

# Données de test (gardées pour compatibilité)
new_data = {
    'cement_co2_pct': [5.2],
    'coal_co2_pct': [32.1],
    'flaring_co2_pct': [1.2],
    'gas_co2_pct': [22.5],
    'oil_co2_pct': [30.0],
    'other_industry_co2_pct': [9.0],
    'cement_co2': [25.3],
    'coal_co2': [150.0],
    'consumption_co2': [310.5],
    'flaring_co2': [5.0],
    'gas_co2': [110.0],
    'oil_co2': [140.0],
    'other_industry_co2': [42.0],
    'co2': [482.3],
    'co2_per_capita': [7.1],
    'cement_co2_per_capita': [0.3],
    'coal_co2_per_capita': [2.1],
    'consumption_co2_per_capita': [3.9],
    'flaring_co2_per_capita': [0.05],
    'gas_co2_per_capita': [1.4],
    'oil_co2_per_capita': [2.1],
    'other_co2_per_capita': [0.5],
    'methane_per_capita': [0.22],
    'nitrous_oxide_per_capita': [0.07],
    'methane': [12.0],
    'nitrous_oxide': [3.1],
    'country': ['X'],
    'year': [2024],
    'iso_code': ['FRA'],
    'population': [68000000]
}

new_df = pd.DataFrame(new_data)

# --------------------------
# 5️⃣ Pipeline automatique
# --------------------------
file_path_d3 = r"data\owid-co2-data.csv"

# Créer dossier hash
hash_dir = "hash"
os.makedirs(hash_dir, exist_ok=True)
hash_file = os.path.join(hash_dir, "hash_d3.txt")

# Vérifier hash précédent
if os.path.exists(hash_file):
    with open(hash_file, "r") as f:
        previous_hash = f.read().strip()
else:
    previous_hash = None

print(" Vérification des changements...")

df3 = clean_dataset_d3(file_path_d3)
output_dir = "staging_csv"
os.makedirs(output_dir, exist_ok=True)  # créer le dossier si nécessaire

csv_file = os.path.join(output_dir, "staging_d3.csv")
df3.to_csv(csv_file, index=False, encoding='utf-8')
print(f" Données du staging sauvegardées dans {csv_file}")

if df3 is None or df3.empty:
    print(" Attention : df3 est vide, rien à charger.")
else:
    new_hash = compute_hash(df3)

    if new_hash != previous_hash:
        print(" Modifications détectées → Mise à jour PostgreSQL...")
        
        # 1. Ajouter la colonne timestamp si nécessaire
        add_timestamp_column('staging_d3')
        
        # 2. Faire l'UPSERT avec les données principales
        upsert_to_postgres(df3, 'staging_d3')
        
        # 3. UPSERT des données de test (optionnel - commenté)
        # upsert_to_postgres(new_df, 'staging_d3')
        
        # 4. Supprimer les données de test (optionnel - commenté)
        # delete_rows_safe("X")
        
        # 5. Sauvegarder nouveau hash
        with open(hash_file, "w") as f:
            f.write(new_hash)
        
        print(" Mise à jour UPSERT terminée avec succès")
        
    else:
        print(" Aucune modification trouvée, rien à faire.")