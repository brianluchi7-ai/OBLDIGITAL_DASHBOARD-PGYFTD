import pandas as pd
import re
from conexion_mysql import crear_conexion

# ======================================================
# === OBL DIGITAL — Generador FTD_MASTER_CLEAN estable ==
# ======================================================

def limpiar_valor_monto(valor):
    """Limpia texto/moneda y devuelve número como string '123.45' o None."""
    if pd.isna(valor):
        return None
    s = str(valor).strip()
    if s == "":
        return None

    # Quitar símbolos de moneda y basura
    s = re.sub(r"[^\d,.\-]", "", s)

    # Caso tipo 1.234,56 o 1,234.56
    if "." in s and "," in s:
        # Si la coma está después del último punto, asumimos coma decimal
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    # Solo coma -> puede ser decimal
    elif "," in s and "." not in s:
        partes = s.split(",")
        if len(partes[-1]) in (2, 3):  # típicamente centavos
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")

    # Intentar parsear a número
    try:
        float(s)
        return s  # lo dejamos como texto numérico limpio
    except:
        return None  # si no es interpretable, lo dejamos vacío


def primera_fila_parece_encabezado(df):
    """
    Heurística:
    - Si la mayoría de nombres de columnas son 'col', 'unnamed', '0', '1', etc.
    - Y la primera fila tiene varios strings no numéricos.
    Entonces usamos la primera fila como encabezado.
    """
    cols = [str(c).lower() for c in df.columns]
    genericas = sum(
        1 for c in cols
        if c.startswith("col") or "unnamed" in c or c in ("0", "1", "2")
    )
    if genericas >= len(cols) * 0.6:  # 60% genéricas
        fila0 = df.iloc[0]
        textos = 0
        for v in fila0:
            if isinstance(v, str):
                if not re.match(r"^\d{1,4}([/-]\d{1,2}){1,2}$", v):  # no parece fecha simple
                    textos += 1
        return textos >= len(fila0) * 0.5  # mitad o más parecen etiquetas
    return False


def limpiar_encabezados(df, tabla):
    """Aplica la heurística de encabezado a la primera fila solo cuando es necesario."""
    if primera_fila_parece_encabezado(df):
        print(f"🔹 {tabla}: primera fila tomada como encabezado.")
        primera_fila = df.iloc[0].fillna("").astype(str)
        df.columns = primera_fila
        df = df.drop(df.index[0])
    else:
        print(f"🔹 {tabla}: se conservan los encabezados originales.")
    return df


def estandarizar_columnas(df):
    """Normaliza nombres de columnas y hace mapping flexible."""
    rename_map = {
        "data": "date",
        "fecha": "date",
        "date_ftd": "date",
        "fechadep": "date",

        "equipo": "team",
        "team_name": "team",
        "leader_team": "team",
        "team_lader": "team",

        "pais": "country",
        "country_name": "country",

        "agente": "agent",
        "agent_sales": "agent",
        "agent_name": "agent",

        "afiliado": "affiliate",
        "affiliate_name": "affiliate",

        "usuario": "id",
        "id_user": "id",
        "id_usuario": "id",

        "monto": "usd",
        "usd_total": "usd",
        "amount_country": "usd",
    }

    # Lower + underscores
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Renombrar según mapa
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df.rename(columns={old: new}, inplace=True)

    return df


def construir_df_limpio(df, month_label):
    """
    Crea un DataFrame solo con las columnas finales:
    date, id, team, agent, country, affiliate, usd, month_name
    alineadas correctamente.
    """
    cols_finales = ["date", "id", "team", "agent", "country", "affiliate", "usd"]

    df_limpio = pd.DataFrame()

    for col in cols_finales:
        if col in df.columns:
            serie = df[col]
        else:
            serie = pd.Series([None] * len(df))

        if col == "usd":
            serie = serie.apply(limpiar_valor_monto)
        else:
            # Normalizar texto (pero respetar None)
            serie = serie.apply(lambda x: str(x).strip() if pd.notna(x) else None)

        df_limpio[col] = serie

    df_limpio["month_name"] = month_label

    # Quitar filas totalmente vacías en las columnas principales
    df_limpio.replace("", None, inplace=True)
    df_limpio.dropna(how="all", subset=cols_finales, inplace=True)
    df_limpio.reset_index(drop=True, inplace=True)

    return df_limpio


def cargar_tabla(tabla, conexion):
    """Lee, limpia y devuelve un DF ya en formato estándar."""
    print(f"\n===> Leyendo tabla {tabla} ...")
    df = pd.read_sql(f"SELECT * FROM {tabla}", conexion)
    print(f"   🔸 Columnas originales: {list(df.columns)}")
    print(f"   🔸 Registros brutos: {len(df)}")

    df = limpiar_encabezados(df, tabla)
    df = estandarizar_columnas(df)

    # Determinar etiqueta de mes a partir del nombre de la tabla
    # ftds_nov_2025 -> Nov, ftds_oct_2025 -> Oct, etc.
    mes_raw = tabla.replace("ftds_", "").replace("_2025", "")
    month_label = mes_raw[:3].capitalize()  # nov -> Nov, oct -> Oct, sep -> Sep

    df_limpio = construir_df_limpio(df, month_label)

    print(f"   ✅ Filas válidas en {tabla}: {len(df_limpio)}")
    return df_limpio


def obtener_datos():
    conexion = crear_conexion()
    if conexion is None:
        print("❌ No se pudo conectar a Railway.")
        return pd.DataFrame()

    tablas = ["ftds_sep_PGY_2025", "ftds_oct_PGY_2025", "ftds_nov_PGY_2025", "ftds_PGY_2025"]
    dataframes = []

    for tabla in tablas:
        try:
            df_mes = cargar_tabla(tabla, conexion)
            if not df_mes.empty:
                dataframes.append(df_mes)
        except Exception as e:
            print(f"⚠️ Error procesando {tabla}: {e}")

    conexion.close()

    if not dataframes:
        print("❌ No se generó FTD_MASTER_PGY (sin datos).")
        return pd.DataFrame()

    # Concatenar todos los meses (ya con mismas columnas)
    df_master = pd.concat(dataframes, ignore_index=True)
    print(f"\n📊 FTD_MASTER generado correctamente con {len(df_master)} registros totales.")
    print(df_master["month_name"].value_counts())

    # Guardar CSV limpio
    df_master.to_csv("FTD_MASTER_PGY_preview.csv", index=False, encoding="utf-8-sig")
    print("💾 Vista previa guardada: FTD_MASTER_PGY_preview.csv")

    # ================================
    # Crear tabla FTD_MASTER_CLEAN
    # ================================
    try:
        conexion = crear_conexion()
        if conexion:
            cursor = conexion.cursor()
            cursor.execute("DROP TABLE IF EXISTS FTD_MASTER_PGY_CLEAN;")
            cursor.execute("""
                CREATE TABLE FTD_MASTER_PGY_CLEAN (
                    date TEXT,
                    id TEXT,
                    team TEXT,
                    agent TEXT,
                    country TEXT,
                    affiliate TEXT,
                    usd TEXT,
                    month_name TEXT
                );
            """)
            conexion.commit()

            columnas = ["date", "id", "team", "agent", "country", "affiliate", "usd", "month_name"]
            for _, row in df_master.iterrows():
                valores = [row.get(c) if row.get(c) is not None else None for c in columnas]
                cursor.execute(
                    "INSERT INTO FTD_MASTER_PGY_CLEAN (date, id, team, agent, country, affiliate, usd, month_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    valores
                )

            conexion.commit()
            conexion.close()
            print("✅ FTD_MASTER_PGY_CLEAN creada y poblada correctamente en Railway.")
    except Exception as e:
        print(f"⚠️ Error al crear FTD_MASTER_PGY_CLEAN: {e}")

    return df_master


if __name__ == "__main__":
    df = obtener_datos()
    print("\nPrimeras filas de FTD_MASTER_PGY:")
    print(df.head())
