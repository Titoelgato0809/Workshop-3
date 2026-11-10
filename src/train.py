from pathlib import Path
import pandas as pd
import numpy as np
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ============================================================
# RUTAS DE ARTEFACTOS
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DIR_MODELOS = BASE_DIR / "models"
DIR_MODELOS.mkdir(parents=True, exist_ok=True)
RUTA_MODELO_SERIALIZADO = DIR_MODELOS / "happiness_pipeline.joblib"

# ============================================================
# DEFINICIÓN DE FEATURES Y TARGET
# ============================================================
FEATURES = [
    "gdp_per_capita",
    "social_support",
    "health_life_expectancy",
    "freedom",
    "generosity",
    "trust_corruption"
]

TARGET = "happiness_score"

# ============================================================
# PIPELINE DE ENTRENAMIENTO Y EVALUACIÓN
# ============================================================
def entrenar_y_evaluar_modelo(df_master: pd.DataFrame):
    print("==================================================")
    print(" INICIANDO STEP 3 & 4: ENTRENAMIENTO DEL MODELO   ")
    print("==================================================\n")
    
    # 1. Separar Predictores (X) y Objetivo (y)
    X = df_master[FEATURES]
    y = df_master[TARGET]
    
    # 2. Divisón Train / Test (80% entrenamiento, 20% prueba)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42
    )
    print(f"Conjunto de Entrenamiento: {X_train.shape[0]} muestras")
    print(f"Conjunto de Evaluación:    {X_test.shape[0]} muestras\n")
    
    # 3. Creación del Pipeline Reproducible (Escalado + Modelo)
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('regressor', LinearRegression())
    ])
    
    # 4. Ajustar Pipeline sobre Train
    print("Entrenando Scikit-Learn Pipeline...")
    pipeline.fit(X_train, y_train)
    
    # 5. Evaluación del Modelo sobre Test
    y_pred = pipeline.predict(X_test)
    
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print("\n--- MÉTRICAS DE RENDIMIENTO (TEST SET) ---")
    print(f"R² Score (Varianza Explicada): {r2:.4f}")
    print(f"RMSE (Error Cuadrático Medio):  {rmse:.4f}")
    print(f"MAE (Error Absoluto Medio):     {mae:.4f}")
    print("------------------------------------------\n")
    
    # 6. Serialización del Pipeline
    joblib.dump(pipeline, RUTA_MODELO_SERIALIZADO)
    print(f"¡Pipeline exportado exitosamente en: {RUTA_MODELO_SERIALIZADO.resolve()}\n")
    
    return pipeline

# ============================================================
# EJECUCIÓN DEL FLUJO INTEGRADO (EXTRACT -> TRANSFORM -> TRAIN)
# ============================================================
if __name__ == "__main__":
    from extract import extraer_2015, extraer_2016, extraer_2017, extraer_2018, extraer_2019
    from transform import transformar_y_unificar
    
    # 1. Extracción
    dict_raw = {
        "2015": extraer_2015(),
        "2016": extraer_2016(),
        "2017": extraer_2017(),
        "2018": extraer_2018(),
        "2019": extraer_2019()
    }
    
    # 2. Transformación y Armonización
    df_master = transformar_y_unificar(dict_raw)
    
    # 3 y 4. Feature Engineering y Entrenamiento
    pipeline_entrenado = entrenar_y_evaluar_modelo(df_master)