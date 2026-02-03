import os
import pandas as pd
from datetime import time


INPUT_FILE = "data/processed/dataset_normalizado.csv"
OUTPUT_FILE = "data/processed/dataset_final.csv"
CHUNK_SIZE = 100_000

NUMERIC_COLUMNS = ["pax_pagos", "pax_pases_pagos", "pax_franq", "pax_TOTAL"]
KEY_COLUMNS = ["FECHA", "DESDE", "HASTA", "LINEA", "ESTACION"]


# =========================
# HORARIOS DE OPERACIÓN
# =========================
WEEKDAY_START = time(5, 30)
WEEKDAY_END = time(23, 30)

SATURDAY_START = time(5, 15)
SATURDAY_END = time(23, 30)

SUNDAY_START = time(7, 45)
SUNDAY_END = time(22, 30)


# =====================================================
# LOAD
# =====================================================
def load_csv_in_chunks(filepath: str, chunk_size: int) -> pd.DataFrame:
    chunks = []
    total_rows = 0

    for i, chunk in enumerate(pd.read_csv(filepath, sep=";", chunksize=chunk_size)):
        chunks.append(chunk)
        total_rows += len(chunk)
        if (i + 1) % 50 == 0:
            print(f"  - Chunks procesados: {i + 1}, Registros: {total_rows:,}")

    df = pd.concat(chunks, ignore_index=True)
    print(f"OK - Dataset cargado: {total_rows:,} registros")
    return df


# =====================================================
# PROFILE
# =====================================================
def profile_dataset(df: pd.DataFrame) -> dict:
    print("\n=== DATASET PROFILE ===")

    print(f"Dimensiones: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    print(f"Memoria usada: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    print("\nTipos de datos:")
    for col, dtype in df.dtypes.items():
        print(f"  {col:20s}: {dtype}")

    nulls = df.isnull().sum()
    duplicates_full = df.duplicated().sum()
    duplicates_key = df.duplicated(subset=KEY_COLUMNS).sum()

    print("\nValores nulos:")
    for col in df.columns:
        count = nulls[col]
        pct = (count / len(df)) * 100
        status = "OK" if count == 0 else "ERROR"
        print(f"  {status:5s} {col:20s}: {count:,} ({pct:.2f}%)")

    print(f"\nDuplicados completos: {duplicates_full:,}")
    print(f"Duplicados por claves {KEY_COLUMNS}: {duplicates_key:,}")

    print("\nValores únicos por columna:")
    for col in df.columns:
        print(f"  {col:20s}: {df[col].nunique():,}")

    return {
        "nulls": nulls.sum(),
        "duplicates_full": duplicates_full,
        "duplicates_key": duplicates_key,
    }


# =====================================================
# FILTRO HORARIO
# =====================================================
def filter_by_operating_hours(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Parse correcto según tu pipeline real
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df["DESDE"] = pd.to_datetime(df["DESDE"], format="%H:%M:%S", errors="coerce").dt.time

    before = len(df)

    weekday_mask = (
        (df["FECHA"].dt.weekday < 5)
        & (df["DESDE"] >= WEEKDAY_START)
        & (df["DESDE"] <= WEEKDAY_END)
    )

    saturday_mask = (
        (df["FECHA"].dt.weekday == 5)
        & (df["DESDE"] >= SATURDAY_START)
        & (df["DESDE"] <= SATURDAY_END)
    )

    sunday_mask = (
        (df["FECHA"].dt.weekday == 6)
        & (df["DESDE"] >= SUNDAY_START)
        & (df["DESDE"] <= SUNDAY_END)
    )

    df_clean = df[weekday_mask | saturday_mask | sunday_mask]

    removed = before - len(df_clean)
    print(f"  - Registros fuera de horario eliminados: {removed:,}")

    return df_clean


# =====================================================
# CLEAN
# =====================================================
def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    # 1️⃣ Filtro horario
    df_clean = filter_by_operating_hours(df_clean)

    # 2️⃣ Duplicados completos
    dup_full = df_clean.duplicated().sum()
    if dup_full > 0:
        print(f"  - Eliminando {dup_full:,} duplicados completos")
        df_clean = df_clean.drop_duplicates()

    # 3️⃣ Duplicados por claves
    dup_key = df_clean.duplicated(subset=KEY_COLUMNS).sum()
    if dup_key > 0:
        print(f"  - Eliminando {dup_key:,} duplicados por claves {KEY_COLUMNS}")
        df_clean = df_clean.drop_duplicates(subset=KEY_COLUMNS, keep="first")

    # 4️⃣ Nulos
    nulls_before = df_clean.isnull().sum().sum()
    if nulls_before > 0:
        print(f"  - Manejando {nulls_before:,} valores nulos")
        for col in NUMERIC_COLUMNS:
            df_clean[col] = df_clean[col].fillna(0)
        df_clean = df_clean.dropna()

    return df_clean


# =====================================================
# SAVE
# =====================================================
def save_clean_dataset(df_clean: pd.DataFrame):
    df_clean.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8")

    print("\n=== DATASET LIMPIO ===")
    print(f"Registros finales: {len(df_clean):,}")
    print(f"Archivo guardado: {OUTPUT_FILE}")
    print(f"Tamaño: {os.path.getsize(OUTPUT_FILE) / 1024**2:.2f} MB")


# =====================================================
# MAIN
# =====================================================
def main():
    print("=" * 60)
    print("VALIDACIÓN Y LIMPIEZA - MOLINETES SUBTE CABA 2024")
    print("=" * 60)

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"No se encontró el archivo: {INPUT_FILE}")

    print(f"Archivo de entrada: {INPUT_FILE}")
    print(f"Tamaño: {os.path.getsize(INPUT_FILE) / 1024**2:.2f} MB")

    print("\nCargando dataset...")
    df = load_csv_in_chunks(INPUT_FILE, CHUNK_SIZE)

    profile_dataset(df)

    print("\nAplicando limpieza...")
    df_clean = clean_dataset(df)

    save_clean_dataset(df_clean)

    print("\n" + "=" * 60)
    print("PROCESO COMPLETADO")
    print("=" * 60)


if __name__ == "__main__":
    main()