from pathlib import Path
import pandas as pd

# ============================================================
# CONFIGURACIÓN DE RUTAS Y EXPORTACIÓN
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DIR_PROCESADOS = BASE_DIR / "data" / "processed"
DIR_PROCESADOS.mkdir(parents=True, exist_ok=True)
RUTA_CSV_FINAL = DIR_PROCESADOS / "happiness_master_processed.csv"

# ============================================================
# DICCIONARIOS DE MAPEO INDIVIDUALES
# ============================================================
MAPEO_2015 = {
    "Country": "country",
    "Happiness Rank": "happiness_rank",
    "Happiness Score": "happiness_score",
    "Economy (GDP per Capita)": "gdp_per_capita",
    "Family": "social_support",
    "Health (Life Expectancy)": "health_life_expectancy",
    "Freedom": "freedom",
    "Generosity": "generosity",
    "Trust (Government Corruption)": "trust_corruption"
}

MAPEO_2016 = MAPEO_2015.copy()

MAPEO_2017 = {
    "Country": "country",
    "Happiness.Rank": "happiness_rank",
    "Happiness.Score": "happiness_score",
    "Economy..GDP.per.Capita.": "gdp_per_capita",
    "Family": "social_support",
    "Health..Life.Expectancy.": "health_life_expectancy",
    "Freedom": "freedom",
    "Generosity": "generosity",
    "Trust..Government.Corruption.": "trust_corruption"
}

MAPEO_2018_2019 = {
    "Country or region": "country",
    "Overall rank": "happiness_rank",
    "Score": "happiness_score",
    "GDP per capita": "gdp_per_capita",
    "Social support": "social_support",
    "Healthy life expectancy": "health_life_expectancy",
    "Freedom to make life choices": "freedom",
    "Generosity": "generosity",
    "Perceptions of corruption": "trust_corruption"
}

DICCIONARIO_MAPEOS = {
    "2015": MAPEO_2015,
    "2016": MAPEO_2016,
    "2017": MAPEO_2017,
    "2018": MAPEO_2018_2019,
    "2019": MAPEO_2018_2019
}

COLUMNAS_CANONICAS = [
    "country", "year", "happiness_rank", "happiness_score",
    "gdp_per_capita", "social_support", "health_life_expectancy",
    "freedom", "generosity", "trust_corruption"
]

# ============================================================
# FUNCIONES DE ARMONIZACIÓN Y LIMPIEZA
# ============================================================
def estandarizar_dataframe(df: pd.DataFrame, año: str) -> pd.DataFrame:
    """Renombra columnas hacia el esquema canónico y filtra las no requeridas."""
    mapeo = DICCIONARIO_MAPEOS[año]
    df_clean = df.copy()
    
    # 1. Renombrar según mapeo del año
    df_clean = df_clean.rename(columns=mapeo)
    
    # 2. Agregar dimensión temporal
    df_clean["year"] = int(año)
    
    # 3. Filtrar únicamente el esquema intersección
    df_clean = df_clean[COLUMNAS_CANONICAS]
    
    return df_clean

def imputar_valores_faltantes(df: pd.DataFrame) -> pd.DataFrame:
    """Trata valores faltantes aplicando la mediana de la columna afectada."""
    df_imputado = df.copy()
    if df_imputado["trust_corruption"].isnull().any():
        mediana_corrupcion = df_imputado["trust_corruption"].median()
        df_imputado["trust_corruption"] = df_imputado["trust_corruption"].fillna(mediana_corrupcion)
        print(f" -> Imputación exitosa: 'trust_corruption' llenado con mediana ({mediana_corrupcion:.4f})")
    return df_imputado

def estandarizar_tipos_datos(df: pd.DataFrame) -> pd.DataFrame:
    """Forzado estricto de tipos de datos sobre el DataFrame unificado."""
    df_typed = df.copy()
    
    # Casting explícito de tipos
    df_typed["country"] = df_typed["country"].astype(str)
    df_typed["year"] = df_typed["year"].astype("int64")
    df_typed["happiness_rank"] = df_typed["happiness_rank"].astype("int64")
    
    columnas_float = [
        "happiness_score", "gdp_per_capita", "social_support",
        "health_life_expectancy", "freedom", "generosity", "trust_corruption"
    ]
    
    for col in columnas_float:
        df_typed[col] = df_typed[col].astype("float64")
        
    return df_typed

def transformar_y_unificar(dict_dfs: dict) -> pd.DataFrame:
    """Ejecuta el pipeline completo de limpieza y armonización sobre los 5 DataFrames."""
    dfs_procesados = []
    print("Iniciando Fase de Transformación y Armonización...\n")
    
    for año, df in dict_dfs.items():
        # Estandarización y filtrado de columnas
        df_armonizado = estandarizar_dataframe(df, año)
        
        # Imputación de nulos si existen en el año actual
        df_armonizado = imputar_valores_faltantes(df_armonizado)
        
        dfs_procesados.append(df_armonizado)
        print(f"[{año}] -> Armonizado a {df_armonizado.shape[1]} columnas | {df_armonizado.shape[0]} filas")

    # Concatenación en dataset maestro
    df_master = pd.concat(dfs_procesados, ignore_index=True)
    
    # Estandarización final de tipos
    df_master = estandarizar_tipos_datos(df_master)
    
    # EXPORTACIÓN A CSV
    df_master.to_csv(RUTA_CSV_FINAL, index=False, encoding="utf-8")
    
    print("\n--- RESUMEN DATASET MAESTRO ARMONIZADO ---")
    print(f"Dimensiones Totales: {df_master.shape[0]} filas × {df_master.shape[1]} columnas")
    print(f"Valores Nulos Totales: {df_master.isnull().sum().sum()}")
    print(f"Archivo guardado exitosamente en: {RUTA_CSV_FINAL.resolve()}")
    
    return df_master

# ============================================================
# EJECUCIÓN DEL MÓDULO (PRUEBA LOCAL)
# ============================================================
if __name__ == "__main__":
    from extract import extraer_2015, extraer_2016, extraer_2017, extraer_2018, extraer_2019
    
    # Extracción previa
    dict_raw = {
        "2015": extraer_2015(),
        "2016": extraer_2016(),
        "2017": extraer_2017(),
        "2018": extraer_2018(),
        "2019": extraer_2019()
    }
    
    # Transformación y armonización
    df_final = transformar_y_unificar(dict_raw)
    print("\nTipos de datos finales:")
    print(df_final.dtypes)