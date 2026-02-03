import pandas as pd
import os
import glob


def concatenar_csvs():
    """
    Une todos los archivos CSV de data/raw en un único CSV en data/processed
    """

    # scripts/
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    # proyecto raíz
    project_root = os.path.join(scripts_dir, "..")

    input_dir = os.path.join(project_root, "data", "raw")
    output_dir = os.path.join(project_root, "data", "processed")
    os.makedirs(output_dir, exist_ok=True)

    patron_csv = os.path.join(input_dir, "*.csv")
    archivos_csv = glob.glob(patron_csv)

    print(f"📂 Buscando CSV en: {os.path.abspath(input_dir)}")
    print(f"Se encontraron {len(archivos_csv)} archivos CSV para procesar:")
    for archivo in archivos_csv:
        print(f"  - {os.path.basename(archivo)}")

    if not archivos_csv:
        print("❌ No se encontraron archivos CSV para procesar.")
        return

    archivo_salida = os.path.join(output_dir, "dataset_concat.csv")

    columnas_estandar = [
        "FECHA",
        "DESDE",
        "HASTA",
        "LINEA",
        "MOLINETE",
        "ESTACION",
        "pax_pagos",
        "pax_pases_pagos",
        "pax_franq",
        "pax_TOTAL",
    ]

    # Escribir header
    with open(archivo_salida, "w", encoding="utf-8", newline="") as f:
        f.write(";".join(columnas_estandar) + "\n")

    total_registros = 0
    archivos_procesados = 0

    for archivo in archivos_csv:
        try:
            print(f"\nProcesando: {os.path.basename(archivo)}")

            encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(archivo, sep=";", encoding=encoding)
                    print(f"  - Codificación exitosa: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if df is None:
                print("  - ❌ No se pudo leer el archivo")
                continue

            df_procesado = procesar_dataframe(df)

            if df_procesado is not None and len(df_procesado) > 0:
                df_procesado.to_csv(
                    archivo_salida,
                    sep=";",
                    index=False,
                    encoding="utf-8",
                    mode="a",
                    header=False,
                )
                total_registros += len(df_procesado)
                archivos_procesados += 1
                print(f"  - ✅ Registros escritos: {len(df_procesado):,}")

        except Exception as e:
            print(f"❌ Error al procesar {archivo}: {str(e)}")
            continue

    print(f"\n=== RESUMEN ===")
    print(f"Total de archivos procesados: {archivos_procesados}")
    print(f"Total de registros en el archivo final: {total_registros:,}")
    print(f"Archivo guardado como: {archivo_salida}")

    if os.path.exists(archivo_salida):
        tamaño_mb = os.path.getsize(archivo_salida) / (1024 * 1024)
        print(f"Tamaño del archivo: {tamaño_mb:.2f} MB")


def procesar_dataframe(df):
    """
    Procesa un DataFrame según su formato y lo estandariza
    """
    try:
        columnas_objetivo = [
            "FECHA",
            "DESDE",
            "HASTA",
            "LINEA",
            "MOLINETE",
            "ESTACION",
            "pax_pagos",
            "pax_pases_pagos",
            "pax_franq",
            "pax_TOTAL",
        ]

        # Caso 1: Ya viene con columnas correctas
        if len(df.columns) >= 10 and all(col in df.columns for col in columnas_objetivo):
            print("  - Formato estándar detectado")
            return df[columnas_objetivo]

        # Caso 2: Todo en una sola columna
        elif len(df.columns) == 1 and ";" in str(df.columns[0]):
            print("  - Formato de columna única detectado")
            df_split = df.iloc[:, 0].str.split(";", expand=True)
            df_split.columns = df_split.iloc[0]
            df_clean = df_split.iloc[1:].reset_index(drop=True)

            if len(df_clean.columns) >= 10:
                df_clean = df_clean.iloc[:, :10]
                df_clean.columns = columnas_objetivo
                return df_clean

        # Caso 3: Header embebido
        elif len(df.columns) == 4 and "FECHA;DESDE;HASTA" in str(df.columns[0]):
            print("  - Formato de header en primera columna detectado")
            df_split = df.iloc[:, 0].str.split(";", expand=True)
            df_split.columns = df_split.iloc[0]
            df_clean = df_split.iloc[1:].reset_index(drop=True)

            if len(df_clean.columns) >= 10:
                df_clean = df_clean.iloc[:, :10]
                df_clean.columns = columnas_objetivo
                return df_clean

        # Caso 4: Buscar columna con separador
        else:
            print("  - Formato no estándar detectado, intentando procesar")
            for col in df.columns:
                if ";" in str(col) and "FECHA" in str(col):
                    df_split = df[col].str.split(";", expand=True)
                    df_split.columns = df_split.iloc[0]
                    df_clean = df_split.iloc[1:].reset_index(drop=True)

                    if len(df_clean.columns) >= 10:
                        df_clean = df_clean.iloc[:, :10]
                        df_clean.columns = columnas_objetivo
                        return df_clean

        print("  - ❌ No se pudo procesar el formato del archivo")
        return None

    except Exception as e:
        print(f"  - ❌ Error al procesar DataFrame: {str(e)}")
        return None


if __name__ == "__main__":
    concatenar_csvs()