from pathlib import Path
import pandas as pd

# ============================================================
# 1. CONFIGURACIÓN DE RUTA
# ============================================================
RUTA_CARPETA = Path(__file__).resolve().parent.parent / "data" / "raw"

# ============================================================
# 2. FUNCIONES DE EXTRACCIÓN INDIVIDUALES
# ============================================================

def extraer_2015() -> pd.DataFrame:
    ruta = RUTA_CARPETA / "2015.csv"
    if not ruta.exists(): raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except:
        df = pd.read_csv(ruta, encoding="latin-1", sep=";")
    print(f"[2015] -> Extraído: {df.shape[0]} filas | {df.shape[1]} columnas")
    return df

def extraer_2016() -> pd.DataFrame:
    ruta = RUTA_CARPETA / "2016.csv"
    if not ruta.exists(): raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except:
        df = pd.read_csv(ruta, encoding="latin-1", sep=";")
    print(f"[2016] -> Extraído: {df.shape[0]} filas | {df.shape[1]} columnas")
    return df

def extraer_2017() -> pd.DataFrame:
    ruta = RUTA_CARPETA / "2017.csv"
    if not ruta.exists(): raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except:
        df = pd.read_csv(ruta, encoding="latin-1", sep=";")
    print(f"[2017] -> Extraído: {df.shape[0]} filas | {df.shape[1]} columnas")
    return df

def extraer_2018() -> pd.DataFrame:
    ruta = RUTA_CARPETA / "2018.csv"
    if not ruta.exists(): raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except:
        df = pd.read_csv(ruta, encoding="latin-1", sep=";")
    print(f"[2018] -> Extraído: {df.shape[0]} filas | {df.shape[1]} columnas")
    return df

def extraer_2019() -> pd.DataFrame:
    ruta = RUTA_CARPETA / "2019.csv"
    if not ruta.exists(): raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
    try:
        df = pd.read_csv(ruta, encoding="utf-8")
    except:
        df = pd.read_csv(ruta, encoding="latin-1", sep=";")
    print(f"[2019] -> Extraído: {df.shape[0]} filas | {df.shape[1]} columnas")
    return df

# ============================================================
# 3. EJECUCIÓN 
# ============================================================
if __name__ == "__main__":
    print("Iniciando extracción por archivo...\n")
    
    # Cargamos cada año en su propia variable independiente
    df_2015 = extraer_2015()
    df_2016 = extraer_2016()
    df_2017 = extraer_2017()
    df_2018 = extraer_2018()
    df_2019 = extraer_2019()
    
    print("\n--- COMPARATIVA DE COLUMNAS POR AÑO ---")
    print(f"\n2015: {df_2015.columns.tolist()}")
    print(f"\n2016: {df_2016.columns.tolist()}")
    print(f"\n2017: {df_2017.columns.tolist()}")
    print(f"\n2018: {df_2018.columns.tolist()}")
    print(f"\n2019: {df_2019.columns.tolist()}") # Imprimimos 2015 y 2019 para ver el contraste inicial