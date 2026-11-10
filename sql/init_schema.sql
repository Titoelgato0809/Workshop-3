-- ============================================================
-- MODELO DIMENSIONAL: MONITOREO DE MACHINE LEARNING
-- ============================================================

-- 1. Dimensión: País
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- 2. Dimensión: Fecha (Calendario)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY, 
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week VARCHAR(20) NOT NULL
);

-- 3. Tabla de Hechos: Predicciones
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id VARCHAR(50) UNIQUE NOT NULL,
    country_id INT REFERENCES dim_country(country_id),
    date_id INT REFERENCES dim_date(date_id),
    gdp FLOAT,
    family FLOAT,
    health FLOAT,
    freedom FLOAT,
    generosity FLOAT,
    corruption FLOAT,
    actual_score FLOAT,
    predicted_score FLOAT,  
    prediction_error FLOAT,
    event_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP,
    processing_delay_seconds FLOAT
);

-- 4. Tabla de Eventos Rechazados (Dead Letter Queue)
CREATE TABLE IF NOT EXISTS rejected_happiness_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50),
    raw_event TEXT,
    rejection_reason TEXT,
    rejected_at TIMESTAMP
);