import pandas as pd
from sqlalchemy import create_engine
import urllib

# =====================
# CONFIG
# =====================
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Subtes;"
    "Trusted_Connection=yes;"
)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

# =====================
# LOAD DATASET
# =====================
df = pd.read_csv("data/processed/dataset_final.csv", sep=";")

df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
df["DESDE"] = pd.to_datetime(df["DESDE"], format="%H:%M:%S").dt.time
df["HASTA"] = pd.to_datetime(df["HASTA"], format="%H:%M:%S").dt.time

print(f"📥 Dataset cargado: {len(df):,} filas")

# =====================
# DIMENSION TIEMPO
# =====================
print("🕒 Poblando dimensión Tiempo...")

tiempo = (
    df[["FECHA"]]
    .drop_duplicates()
    .rename(columns={"FECHA": "fecha"})
)
tiempo["dia"] = tiempo["fecha"].apply(lambda x: x.day)
tiempo["mes"] = tiempo["fecha"].apply(lambda x: x.month)
tiempo["trimestre"] = tiempo["fecha"].apply(lambda x: (x.month - 1) // 3 + 1)
tiempo["semestre"] = tiempo["fecha"].apply(lambda x: (x.month - 1) // 6 + 1)
tiempo["anio"] = tiempo["fecha"].apply(lambda x: x.year)

with engine.begin() as conn:
    existentes = pd.read_sql("SELECT fecha FROM Tiempo", conn)
    existentes["fecha"] = pd.to_datetime(existentes["fecha"]).dt.date
    tiempo = tiempo[~tiempo["fecha"].isin(existentes["fecha"])]

    if not tiempo.empty:
        tiempo.to_sql("Tiempo", conn, if_exists="append", index=False)

print("✅ Tiempo OK")

# =====================
# DIMENSION TURNO
# =====================
print("⏰ Poblando dimensión Turno...")

turno = (
    df[["DESDE", "HASTA"]]
    .drop_duplicates()
    .rename(columns={"DESDE": "desde", "HASTA": "hasta"})
)

with engine.begin() as conn:
    existentes = pd.read_sql("SELECT desde, hasta FROM Turno", conn)
    turno = turno.merge(existentes, on=["desde", "hasta"], how="left", indicator=True)
    turno = turno[turno["_merge"] == "left_only"].drop(columns="_merge")

    if not turno.empty:
        turno.to_sql("Turno", conn, if_exists="append", index=False)

print("✅ Turno OK")

# =====================
# DIMENSION ESTACION
# =====================
print("🚉 Poblando dimensión Estacion...")

estacion = (
    df[["LINEA", "ESTACION"]]
    .drop_duplicates()
    .rename(columns={"LINEA": "linea_subte", "ESTACION": "nombre_estacion"})
)

with engine.begin() as conn:
    existentes = pd.read_sql("SELECT linea_subte, nombre_estacion FROM Estacion", conn)
    estacion = estacion.merge(
        existentes,
        on=["linea_subte", "nombre_estacion"],
        how="left",
        indicator=True
    )
    estacion = estacion[estacion["_merge"] == "left_only"].drop(columns="_merge")

    if not estacion.empty:
        estacion.to_sql("Estacion", conn, if_exists="append", index=False)

print("✅ Estacion OK")

# =====================
# CARGAR IDS DE DIMENSIONES
# =====================
print("🔗 Resolviendo claves foráneas...")

tiempo_db = pd.read_sql("SELECT id_tiempo, fecha FROM Tiempo", engine)
tiempo_db["fecha"] = pd.to_datetime(tiempo_db["fecha"]).dt.date

turno_db = pd.read_sql("SELECT id_turno, desde, hasta FROM Turno", engine)
estacion_db = pd.read_sql("SELECT id_estacion, linea_subte, nombre_estacion FROM Estacion", engine)

df_fact = (
    df.merge(tiempo_db, left_on="FECHA", right_on="fecha", how="left")
      .merge(turno_db, left_on=["DESDE", "HASTA"], right_on=["desde", "hasta"], how="left")
      .merge(estacion_db, left_on=["LINEA", "ESTACION"], right_on=["linea_subte", "nombre_estacion"], how="left")
)

fact_registros = df_fact[[
    "id_tiempo", "id_turno", "id_estacion",
    "pax_pagos", "pax_pases_pagos", "pax_franq", "pax_TOTAL"
]]

# =====================
# INSERT FACT REGISTROS (IDEMPOTENTE)
# =====================
print("📊 Poblando tabla Registros...")

with engine.begin() as conn:
    existentes = pd.read_sql("""
        SELECT id_tiempo, id_turno, id_estacion
        FROM Registros
    """, conn)

    fact_registros = fact_registros.merge(
        existentes,
        on=["id_tiempo", "id_turno", "id_estacion"],
        how="left",
        indicator=True
    )

    fact_registros = fact_registros[fact_registros["_merge"] == "left_only"].drop(columns="_merge")

    if not fact_registros.empty:
        fact_registros.to_sql("Registros", conn, if_exists="append", index=False)

print("🎉 Pipeline DW ejecutado correctamente.")