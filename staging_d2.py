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
def clean_dataset(file_path_d1=None, file_path_d2=None):
    # --------------------------
    # 1️⃣ Nettoyage staging_d1
    # --------------------------
    df1=None
    if file_path_d1:
        df1 = pd.read_excel(file_path_d1, sheet_name='Source_Apportionment_DB_WHO')
        df1 = df1.iloc[:527]  # si tu veux limiter
        df1 = df1.rename(columns={
            'Site Location': 'city',
            'Population estimate *': 'population',
            'Country': 'country',
            'ISO 3 code': 'iso3',
            'Country Region': 'region',
            'continent': 'continent',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Site typology': 'site_typology',
            'PM10 µgm-3 *': 'concentration_pm10',
            'PM2.5 µgm-3 *': 'concentration_pm25',
            'Methodology': 'methodology',
            'Reference author': 'reference_author',
            'Reference year': 'study_year',
            'Study year': 'year',
            'Season': 'season',
            'SEA SALT%': 'sea_salt',
            'DUST%': 'dust',
            'TRAFFIC%': 'traffic',
            'INDUSTRY%': 'industry',
            'BIOM. BURN./RES.%': 'biomass_burn',
            'OTHER (unspecified-human origin)%': 'other_source'
        })

        # Conversion types
        text_cols = ['city','country','iso3','region','continent','site_typology','methodology','reference_author','season']
        int_cols = ['population','year','study_year']
        float_cols = ['latitude','longitude','concentration_pm10','concentration_pm25','sea_salt','dust','traffic','industry','biomass_burn','other_source']

        for col in text_cols:
            df1[col] = df1[col].astype('string')
        for col in int_cols:
            df1[col] = pd.to_numeric(df1[col], errors='coerce').fillna(0).astype(int)
        for col in float_cols:
            df1[col] = pd.to_numeric(df1[col], errors='coerce')

        # Agrégation
        group_cols = ['country', 'city', 'year']
        numeric_avg_cols = ['concentration_pm10', 'concentration_pm25',
                            'sea_salt', 'dust', 'traffic', 'industry',
                            'biomass_burn', 'other_source']
        first_cols = ['population', 'iso3', 'region', 'continent', 'latitude', 'longitude', 'site_typology']
        concat_cols = ['methodology', 'reference_author', 'study_year', 'season']

        agg_dict = {col: 'mean' for col in numeric_avg_cols}
        agg_dict.update({col: 'first' for col in first_cols})
        agg_dict.update({col: lambda x: ' + '.join(x.dropna().astype(str).unique()) for col in concat_cols})

        df1 = df1.groupby(group_cols, as_index=False).agg(agg_dict)

        # Fallback valeurs manquantes
        for col in ['concentration_pm10', 'concentration_pm25']:
            df1[col] = df1.groupby(['city','year'])[col].transform(lambda x: x.fillna(x.mean()))
            df1[col] = df1.groupby(['country','year'])[col].transform(lambda x: x.fillna(x.mean()))
            df1[col] = df1.groupby(['country'])[col].transform(lambda x: x.fillna(x.mean()))

        source_cols = ['sea_salt','traffic','industry','dust','biomass_burn','other_source']
        for col in source_cols:
            df1[col] = df1.groupby(['city','year'])[col].transform(lambda x: x.fillna(x.mean()))
            df1[col] = df1.groupby(['city'])[col].transform(lambda x: x.fillna(x.mean()))
            df1[col] = df1.groupby(['country','year'])[col].transform(lambda x: x.fillna(x.mean()))
            df1[col] = df1.groupby(['country'])[col].transform(lambda x: x.fillna(x.mean()))

    # --------------------------
    # 2️⃣ Nettoyage staging_d2
    # --------------------------

    if file_path_d2:
        df2 = pd.read_excel(file_path_d2)

        def expand_years_dash_only(value):
            if pd.isna(value) or str(value).strip() in ['-', '????', '']:
                return []
            years = []
            for p in str(value).split('-'):
                try:
                    years.append(int(float(p)))
                except:
                    pass
            return years if years else []

        def split_rows_by_dash_years(df, col='year'):
            rows = []
            for _, row in df.iterrows():
                year_values = expand_years_dash_only(row[col])
                if not year_values:
                    new_row = row.copy()
                    new_row[col] = pd.NA
                    rows.append(new_row)
                else:
                    for y in year_values:
                        new_row = row.copy()
                        new_row[col] = y
                        rows.append(new_row)
            return pd.DataFrame(rows)

        # Étape 1 : Expansion des années
        df2 = split_rows_by_dash_years(df2, col='study_year')
        
        # Étape 2 : Standardiser les noms de colonnes (minuscules, sans espaces)
        df2.columns = [str(col).lower().strip().replace(' ', '_') for col in df2.columns]
        
        # Étape 3 : Définir le mapping des colonnes attendues
        column_mapping = {
            'concentration_pm10': 'concentration_pm10',
            'concentration_pm25': 'concentration_pm25', 
            'pm10': 'concentration_pm10',
            'pm2.5': 'concentration_pm25',
            'pm25': 'concentration_pm25',
            'sea_salt': 'sea_salt',
            'dust': 'dust',
            'traffic': 'traffic',
            'industry': 'industry',
            'biomass_burn': 'biomass_burn',
            'biomass': 'biomass_burn',
            'other_source': 'other_source',
            'other': 'other_source',
            'methodology': 'methodology',
            'reference_author': 'reference_author',
            'site_typology': 'site_typology',
            'population': 'population',
            'iso3': 'iso3',
            'region': 'region',
            'continent': 'continent',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'season': 'season',
            'country': 'country',
            'city': 'city',
            'year': 'year',
            'study_year': 'study_year'
        }
        
        # Renommer les colonnes selon le mapping
        df2 = df2.rename(columns={k: v for k, v in column_mapping.items() if k in df2.columns})
        
        # Étape 4 : Définir les colonnes attendues avec leurs types
        expected_columns = {
            'country': 'string',
            'city': 'string',
            'year': 'int64',
            'study_year': 'int64',
            'concentration_pm10': 'float64',
            'concentration_pm25': 'float64',
            'sea_salt': 'float64',
            'dust': 'float64',
            'traffic': 'float64',
            'industry': 'float64',
            'biomass_burn': 'float64',
            'other_source': 'float64',
            'methodology': 'string',
            'reference_author': 'string',
            'site_typology': 'string',
            'population': 'float64',
            'iso3': 'string',
            'region': 'string',
            'continent': 'string',
            'latitude': 'float64',
            'longitude': 'float64',
            'season': 'string'
        }
        
        # S'assurer que toutes les colonnes existent
        for col in expected_columns.keys():
            if col not in df2.columns:
                df2[col] = None
        
        # Étape 5 : Conversion des types
        for col, dtype in expected_columns.items():
            if col in df2.columns:
                try:
                    if dtype == 'string':
                        df2[col] = df2[col].astype(str).replace('nan', '').replace('None', '')
                    elif dtype == 'int64':
                        df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0)
                        df2[col] = df2[col].astype('float64')
                        if (df2[col] % 1 == 0).all():
                            df2[col] = df2[col].astype('int64')
                    elif dtype == 'float64':
                        df2[col] = pd.to_numeric(df2[col], errors='coerce').astype('float64')
                except Exception as e:
                    df2[col] = None if dtype == 'string' else (0 if 'int' in dtype else 0.0)
        
        # Étape 6 : Gestion des doublons sur (country, city, year, methodology)
        key_cols = ['country', 'city', 'year', 'methodology']
        
        # Vérifier les doublons AVANT
        duplicate_count_before = df2.duplicated(subset=key_cols, keep=False).sum()
        
        if duplicate_count_before > 0:            
            # Colonnes numériques à MOYENNER
            numeric_cols = [
                'concentration_pm10', 'concentration_pm25', 'sea_salt', 
                'dust', 'traffic', 'industry', 'biomass_burn', 'other_source',
                'latitude', 'longitude', 'population', 'study_year'
            ]
            
            # Colonnes texte à CONCATÉNER (première valeur non-nulle)
            text_cols = [
                'iso3', 'region', 'continent', 'reference_author', 
                'site_typology', 'season'
            ]
            
            # Créer dictionnaire d'agrégation
            agg_dict = {}
            
            # Pour les colonnes numériques : moyenne
            for col in numeric_cols:
                if col in df2.columns:
                    agg_dict[col] = 'mean'
            
            # Pour les colonnes texte : première valeur non-nulle
            for col in text_cols:
                if col in df2.columns:
                    agg_dict[col] = lambda x: x.dropna().iloc[0] if not x.dropna().empty else ''
            
            # Agrégation
            df2 = df2.groupby(key_cols, as_index=False).agg(agg_dict)
        
        # Vérifier les doublons APRÈS agrégation
        duplicate_count_after = df2.duplicated(subset=key_cols).sum()
        if duplicate_count_after > 0:
            # Supprimer les doublons
            df2 = df2.drop_duplicates(subset=key_cols, keep='first')
        
        # Étape 7 : Forçage final des types
        # Fonction pour forcer le type
        def force_type(series, target_type):
            if target_type == 'float64':
                return pd.to_numeric(series, errors='coerce').astype('float64')
            elif target_type == 'int64':
                temp = pd.to_numeric(series, errors='coerce').fillna(0)
                return temp.astype('int64')
            elif target_type == 'string':
                return series.astype(str).replace('nan', '').replace('None', '')
            return series
        
        # Appliquer à toutes les colonnes
        for col, dtype in expected_columns.items():
            if col in df2.columns:
                df2[col] = force_type(df2[col], dtype)
        
        # Vérifier qu'il n'y a plus de doublons
        final_duplicates = df2.duplicated(subset=key_cols).sum()
        if final_duplicates > 0:
            df2 = df2.drop_duplicates(subset=key_cols, keep='first')
    
    return df1, df2

# --------------------------
# 3️⃣ Fonction hash pour détecter changements
# --------------------------
def compute_hash(df):
    df_bytes = df.to_csv(index=False).encode()
    return hashlib.md5(df_bytes).hexdigest()

# --------------------------
# 4️⃣ Fonctions UPSERT pour PostgreSQL
# --------------------------
def add_timestamp_column(table_name='staging_d2'):
    """Ajoute une colonne timestamp si elle n'existe pas"""
    sql = f"""
    ALTER TABLE etl.{table_name} 
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"  Colonne updated_at vérifiée/ajoutée pour {table_name}")

from sqlalchemy.types import Numeric, Integer, Text
from sqlalchemy import text

def upsert_to_postgres(df, table_name='staging_d2'):
    """UPSERT : met à jour si existe, insère si nouveau"""
    
    # 1. Créer une table temporaire
    temp_table = f"temp_{table_name}"
    
    # Définir les types pour chaque colonne
    dtype_mapping = {
        'country': Text(),
        'city': Text(),
        'year': Integer(),
        'study_year': Integer(),
        'concentration_pm10': Numeric(),
        'concentration_pm25': Numeric(),
        'sea_salt': Numeric(),
        'dust': Numeric(),
        'traffic': Numeric(),
        'industry': Numeric(),
        'biomass_burn': Numeric(),
        'other_source': Numeric(),
        'methodology': Text(),
        'reference_author': Text(),
        'site_typology': Text(),
        'population': Integer(),
        'iso3': Text(),
        'region': Text(),
        'continent': Text(),
        'latitude': Numeric(),
        'longitude': Numeric(),
        'season': Text()
    }
    
    # Garder seulement les colonnes qui existent dans le DataFrame
    existing_dtypes = {k: v for k, v in dtype_mapping.items() if k in df.columns}
    
    df.to_sql(temp_table, engine, 
              schema='etl', 
              if_exists='replace', 
              index=False,
              dtype=existing_dtypes)
    
    # 2. UPSERT avec ON CONFLICT
    upsert_sql = f"""
    INSERT INTO etl.{table_name} (
    country, city, year, methodology, iso3, region, continent,
    concentration_pm10, concentration_pm25, study_year,
    sea_salt, dust, traffic, industry, biomass_burn, other_source,
    reference_author, site_typology, population,
    latitude, longitude, season, updated_at
)
    SELECT     country, city, year, methodology, iso3, region, continent,
    concentration_pm10::numeric, concentration_pm25::numeric, study_year::numeric,
    sea_salt::numeric, dust::numeric, traffic::numeric, industry::numeric, biomass_burn::numeric, other_source::numeric,
    reference_author, site_typology, population::numeric,
    latitude::numeric, longitude::numeric, season, CURRENT_TIMESTAMP
    FROM etl.{temp_table}
    ON CONFLICT (country, city, year, methodology) DO UPDATE SET
        iso3 = EXCLUDED.iso3,
        region = EXCLUDED.region,
        continent = EXCLUDED.continent,
        concentration_pm10 = EXCLUDED.concentration_pm10,
        concentration_pm25 = EXCLUDED.concentration_pm25,
        study_year = EXCLUDED.study_year,
        sea_salt = EXCLUDED.sea_salt,
        dust = EXCLUDED.dust,
        traffic = EXCLUDED.traffic,
        industry = EXCLUDED.industry,
        biomass_burn = EXCLUDED.biomass_burn,
        other_source = EXCLUDED.other_source,
        reference_author = EXCLUDED.reference_author,
        site_typology = EXCLUDED.site_typology,
        population = EXCLUDED.population,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        season = EXCLUDED.season,
        updated_at = CURRENT_TIMESTAMP
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
            text("DELETE FROM etl.staging_d2 WHERE city = :city"),
            {"city": city_name}
        )
    print(f"  Lignes de la ville {city_name} supprimées")

# 5️⃣ Pipeline automatique
# --------------------------
file_path = r"data\database_source_apport_studies_v2_0_26_9_2015.xls"
file_path_d2 = r"data\Book1.xlsx"

# Créer dossier hash
hash_dir = "hash"
os.makedirs(hash_dir, exist_ok=True)
hash_file = os.path.join(hash_dir, "hash_d2.txt")

# Vérifier hash précédent
if os.path.exists(hash_file):
    with open(hash_file, "r") as f:
        previous_hash = f.read().strip()
else:
    previous_hash = None

print(" Vérification des changements...")

_, df2 = clean_dataset(file_path_d1=None, file_path_d2=file_path_d2)
output_dir = "staging_csv"
os.makedirs(output_dir, exist_ok=True)  # créer le dossier si nécessaire

csv_file = os.path.join(output_dir, "staging_d2.csv")
df2.to_csv(csv_file, index=False, encoding='utf-8')
print(f" Données du staging sauvegardées dans {csv_file}")

if df2 is None or df2.empty:
    print(" Attention : df2 est vide, rien à charger.")
else:
    new_hash = compute_hash(df2)

    if new_hash != previous_hash:
        print(" Modifications détectées → Mise à jour PostgreSQL...")
        
        # Ajouter la colonne timestamp
        add_timestamp_column()
        
        # Faire l'UPSERT au lieu de LOAD
        upsert_to_postgres(df2)
        
        # Sauvegarder nouveau hash
        with open(hash_file, "w") as f:
            f.write(new_hash)
        
        print(" Mise à jour UPSERT terminée avec succès")
        
    else:
        print(" Aucune modification trouvée, rien à faire.")