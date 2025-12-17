from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql://myuser:strong_password@localhost:5432/dwh_pollution"
)

# =====================================================
# 1️⃣ Schéma
# =====================================================
def create_schema():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw;"))

# =====================================================
# 2️⃣ Dimensions
# =====================================================
def create_dimensions():
    sql = """
    CREATE TABLE IF NOT EXISTS dw.dim_time (
        time_id SERIAL PRIMARY KEY,
        year INT,
        season TEXT,
        UNIQUE (year, season)
    );

    CREATE TABLE IF NOT EXISTS dw.dim_location (
        location_id SERIAL PRIMARY KEY,
        country TEXT,
        city TEXT,
        region TEXT,
        iso3 TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );

    CREATE TABLE IF NOT EXISTS dw.dim_source (
        source_id SERIAL PRIMARY KEY,
        source_name TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS dw.dim_pollutant (
        pollutant_id SERIAL PRIMARY KEY,
        pollutant_name TEXT UNIQUE,
        unit TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

# =====================================================
# 3️⃣ Tables de faits avec colonne row_hash
# =====================================================
def create_facts():
    sql = """
    CREATE TABLE IF NOT EXISTS dw.fact_air_quality (
        time_id INT REFERENCES dw.dim_time,
        location_id INT REFERENCES dw.dim_location,
        pm10 DOUBLE PRECISION,
        pm25 DOUBLE PRECISION,
        no2 DOUBLE PRECISION,
        population DOUBLE PRECISION,
        station_type TEXT,
        row_hash TEXT,
        PRIMARY KEY (time_id, location_id)
    );

    CREATE TABLE IF NOT EXISTS dw.fact_source_apportionment (
        time_id INT REFERENCES dw.dim_time,
        location_id INT REFERENCES dw.dim_location,
        source_id INT REFERENCES dw.dim_source,
        contribution_pct DOUBLE PRECISION,
        row_hash TEXT,
        PRIMARY KEY (time_id, location_id, source_id)
    );

    CREATE TABLE IF NOT EXISTS dw.fact_emissions (
        time_id INT REFERENCES dw.dim_time,
        location_id INT REFERENCES dw.dim_location,
        co2 DOUBLE PRECISION,
        co2_per_capita DOUBLE PRECISION,
        methane DOUBLE PRECISION,
        nitrous_oxide DOUBLE PRECISION,
        population DOUBLE PRECISION,
        row_hash TEXT,
        PRIMARY KEY (time_id, location_id)
    );

    CREATE TABLE IF NOT EXISTS dw.fact_emissions_by_source (
        time_id INT REFERENCES dw.dim_time,
        location_id INT REFERENCES dw.dim_location,
        source_id INT REFERENCES dw.dim_source,
        co2 DOUBLE PRECISION,
        co2_pct DOUBLE PRECISION,
        row_hash TEXT,
        PRIMARY KEY (time_id, location_id, source_id)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

# =====================================================
# 4️⃣ Chargement dimensions
# =====================================================
def load_dimensions():
    sql = """
    -- DIM TIME 
    INSERT INTO dw.dim_time (year, season)
    SELECT DISTINCT year, 'year' FROM etl.staging_d1 WHERE year IS NOT NULL
    UNION
    SELECT DISTINCT year, COALESCE(season, 'year') FROM etl.staging_d2 WHERE year IS NOT NULL
    UNION
    SELECT DISTINCT year, 'year' FROM etl.staging_d3 WHERE year IS NOT NULL
    ON CONFLICT (year, season) DO NOTHING;

    -- DIM LOCATION
    INSERT INTO dw.dim_location (country, city, region, iso3, latitude, longitude)
    SELECT DISTINCT country, split_part(city, '/', 1), region, iso3, latitude, longitude
    FROM etl.staging_d1
    UNION
    SELECT DISTINCT country, split_part(city, '/', 1), region, iso3, latitude, longitude
    FROM etl.staging_d2
    UNION
    SELECT DISTINCT country, NULL::text, NULL::text, iso_code, NULL::double precision, NULL::double precision
    FROM etl.staging_d3
    ON CONFLICT DO NOTHING;

    -- DIM SOURCE
    INSERT INTO dw.dim_source (source_name)
    VALUES
        ('traffic'), ('industry'), ('biomass_burn'),
        ('dust'), ('sea_salt'), ('other'),
        ('coal'), ('gas'), ('oil'),
        ('cement'), ('flaring'), ('other_industry')
    ON CONFLICT DO NOTHING;

    -- DIM POLLUTANT
    INSERT INTO dw.dim_pollutant (pollutant_name, unit)
    VALUES
        ('pm10', 'µg/m³'), ('pm25', 'µg/m³'), ('no2', 'µg/m³'),
        ('co2','MtCO2'), ('methane','MtCH4'), ('nitrous_oxide', 'MtN2O')
    ON CONFLICT DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

# =====================================================
# 5️⃣ Chargement faits avec hash
# =====================================================
def load_facts():
    # Fact Air Quality
    sql_air = """
    INSERT INTO dw.fact_air_quality AS f
    SELECT
        t.time_id,
        l.location_id,
        s.concentration_pm10,
        s.concentration_pm25,
        s.concentration_no2,
        s.population,
        s.station_type,
        md5(
            COALESCE(s.concentration_pm10::text,'') || '|' ||
            COALESCE(s.concentration_pm25::text,'') || '|' ||
            COALESCE(s.concentration_no2::text,'') || '|' ||
            COALESCE(s.population::text,'') || '|' ||
            COALESCE(s.station_type,'')
        ) AS row_hash
    FROM etl.staging_d1 s
    JOIN dw.dim_time t ON t.year = s.year
    JOIN dw.dim_location l ON l.country = s.country AND l.city = split_part(s.city,'/',1)
    ON CONFLICT (time_id, location_id) DO UPDATE
    SET
        pm10 = EXCLUDED.pm10,
        pm25 = EXCLUDED.pm25,
        no2 = EXCLUDED.no2,
        population = EXCLUDED.population,
        station_type = EXCLUDED.station_type,
        row_hash = EXCLUDED.row_hash
    WHERE f.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """

    # Fact Source Apportionment
    sql_source = """
    INSERT INTO dw.fact_source_apportionment AS f
    SELECT
        t.time_id,
        l.location_id,
        ds.source_id,
        v.val,
        md5(v.val::text) AS row_hash
    FROM etl.staging_d2 s
    JOIN dw.dim_time t ON t.year = s.year
    JOIN dw.dim_location l ON l.country = s.country AND l.city = split_part(s.city,'/',1)
    JOIN LATERAL (
        VALUES
            ('traffic', s.traffic::double precision),
            ('industry', s.industry::double precision),
            ('biomass_burn', s.biomass_burn::double precision),
            ('dust', s.dust::double precision),
            ('sea_salt', s.sea_salt::double precision),
            ('other', s.other_source::double precision)
    ) v(src, val) ON val IS NOT NULL
    JOIN dw.dim_source ds ON ds.source_name = v.src
    ON CONFLICT (time_id, location_id, source_id) DO UPDATE
    SET contribution_pct = EXCLUDED.contribution_pct,
        row_hash = EXCLUDED.row_hash
    WHERE f.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """

    # Fact Emissions
    sql_emissions = """
    INSERT INTO dw.fact_emissions AS f
    SELECT
        t.time_id,
        l.location_id,
        s.co2,
        s.co2_per_capita,
        s.methane,
        s.nitrous_oxide,
        s.population,
        md5(
            COALESCE(s.co2::text,'') || '|' ||
            COALESCE(s.co2_per_capita::text,'') || '|' ||
            COALESCE(s.methane::text,'') || '|' ||
            COALESCE(s.nitrous_oxide::text,'') || '|' ||
            COALESCE(s.population::text,'')
        ) AS row_hash
    FROM etl.staging_d3 s
    JOIN dw.dim_time t ON t.year = s.year
    JOIN dw.dim_location l ON l.country = s.country AND l.city IS NULL
    ON CONFLICT (time_id, location_id) DO UPDATE
    SET
        co2 = EXCLUDED.co2,
        co2_per_capita = EXCLUDED.co2_per_capita,
        methane = EXCLUDED.methane,
        nitrous_oxide = EXCLUDED.nitrous_oxide,
        population = EXCLUDED.population,
        row_hash = EXCLUDED.row_hash
    WHERE f.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """

    # Fact Emissions by Source
    sql_emissions_source = """
    INSERT INTO dw.fact_emissions_by_source AS f
    SELECT
        t.time_id,
        l.location_id,
        ds.source_id,
        v.v_co2,
        v.v_pct,
        md5(v.v_co2::text || '|' || v.v_pct::text) AS row_hash
    FROM etl.staging_d3 s
    JOIN dw.dim_time t ON t.year = s.year
    JOIN dw.dim_location l ON l.country = s.country AND l.city IS NULL
    JOIN LATERAL (
        VALUES
            ('coal', s.coal_co2::double precision, s.coal_co2_pct::double precision),
            ('gas', s.gas_co2::double precision, s.gas_co2_pct::double precision),
            ('oil', s.oil_co2::double precision, s.oil_co2_pct::double precision),
            ('cement', s.cement_co2::double precision, s.cement_co2_pct::double precision),
            ('flaring', s.flaring_co2::double precision, s.flaring_co2_pct::double precision),
            ('other_industry', s.other_industry_co2::double precision, s.other_industry_co2_pct::double precision)
    ) v(src, v_co2, v_pct) ON v.v_co2 IS NOT NULL
    JOIN dw.dim_source ds ON ds.source_name = v.src
    ON CONFLICT (time_id, location_id, source_id) DO UPDATE
    SET co2 = EXCLUDED.co2,
        co2_pct = EXCLUDED.co2_pct,
        row_hash = EXCLUDED.row_hash
    WHERE f.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """

    with engine.begin() as conn:
        conn.execute(text(sql_air))
        conn.execute(text(sql_source))
        conn.execute(text(sql_emissions))
        conn.execute(text(sql_emissions_source))

# =====================================================
# 6️⃣ Exécution
# =====================================================
def run_load_dw():
    print(" Création DW")
    create_schema()
    create_dimensions()
    create_facts()

    print(" Chargement dimensions")
    load_dimensions()

    print(" Chargement faits avec détection des changements")
    load_facts()

    print(" DW prêt pour BI")

if __name__ == "__main__":
    run_load_dw()
