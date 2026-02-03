import pandas as pd


INPUT_PATH = "data/processed/dataset_concat.csv"
OUTPUT_PATH = "data/processed/dataset_normalizado.csv"

STATION_NORMALIZATION = {
    "Callao": "Callao.D",
    "Pueyrredon": "Pueyrredon.B",
    "Retiro": "Retiro.C",
    "Retiro E": "Retiro.E",
    "Independencia": "Independencia.C",
    "Independencia.H": "Independencia.E",
}

NUMERIC_COLUMNS = ["pax_pagos", "pax_pases_pagos", "pax_franq", "pax_TOTAL"]
GROUP_COLUMNS = ["FECHA", "DESDE", "HASTA", "LINEA", "ESTACION"]


def load_data(filepath: str) -> pd.DataFrame:
    """Carga dataset concatenado."""
    return pd.read_csv(filepath, sep=";")


def standardize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas de texto."""
    df = df.copy()
    df["LINEA"] = df["LINEA"].astype(str).str.strip().str.upper()
    df["ESTACION"] = df["ESTACION"].astype(str).str.strip().str.title()
    return df


def normalize_station_names(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige inconsistencias detectadas en nombres de estaciones."""
    df = df.copy()
    df["ESTACION"] = df["ESTACION"].replace(STATION_NORMALIZATION)
    return df


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas que no aportan valor analítico."""
    df = df.copy()
    if "MOLINETE" in df.columns:
        df = df.drop(columns=["MOLINETE"])
    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas numéricas con formatos inconsistentes."""
    df = df.copy()
    for col in NUMERIC_COLUMNS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
            .astype("Int64")
        )
    return df


def aggregate_by_station(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa por fecha, franja horaria, línea y estación."""
    return df.groupby(GROUP_COLUMNS, as_index=False)[NUMERIC_COLUMNS].sum()


def clean_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas de fecha y hora a tipos correctos."""
    df = df.copy()
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce", dayfirst=True)
    df["DESDE"] = pd.to_datetime(df["DESDE"], format="%H:%M:%S", errors="coerce").dt.time
    df["HASTA"] = pd.to_datetime(df["HASTA"], format="%H:%M:%S", errors="coerce").dt.time
    return df


def main():
    print("🚇 Procesando dataset de molinetes...")

    df = load_data(INPUT_PATH)
    print(f"📥 Dataset cargado: {df.shape[0]:,} filas | {df.shape[1]} columnas")

    df = standardize_text_columns(df)
    df = normalize_station_names(df)
    df = drop_unused_columns(df)
    df = clean_numeric_columns(df)
    df = aggregate_by_station(df)
    df = clean_datetime_columns(df)

    print("\n📊 Tipos de datos finales:")
    print(df.dtypes)

    df.to_csv(OUTPUT_PATH, sep=";", index=False)
    print(f"\n✅ Dataset final guardado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()