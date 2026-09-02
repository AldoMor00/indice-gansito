# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run nb_00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze de CONASAMI. Misma regla que Profeco —no castea, no filtra, no deduplica— y la
# misma mecánica de nb_00_config, pero llaveada por (archivo, version): esta fuente no
# tiene período, se versiona por sha256 (decisión #9). Aquí no corre nada.

FUENTE = "conasami"

LLAVES = [("archivo", "_archivo"), ("version", "_version")]

# Un archivo del repo es una tabla de bronze: los dos CSV no comparten una sola columna.
TABLAS = {
    "sm_real_indice": "salario_indice",
    "sm_general_profesionales_zonas": "salario_zonas",
}


def url_de(e: dict) -> str:
    """Espeja ruta() de scripts/ingesta_conasami.py. Una versión > 1 lleva sufijo."""
    sufijo = "" if e["version"] == 1 else f"_v{e['version']}"
    return f"{RAW}/{FUENTE}/salarios/{e['archivo']}{sufijo}.csv"


def baja(e: dict) -> pd.DataFrame:
    """Un CSV del repo de datos, ya con su linaje pegado.

    `dtype=str` y `keep_default_na=False`: bronze no castea, y dejar que pandas infiera
    convertiría `smg_nominal` a float y las celdas vacías de los oficios a NaN. El
    tipado es de silver.
    """
    crudo = requests.get(url_de(e), timeout=60).content
    pdf = pd.read_csv(io.BytesIO(crudo), dtype=str, keep_default_na=False)

    # Contra el manifiesto, antes de escribir nada.
    if len(pdf) != e["filas"]:
        raise RuntimeError(
            f"{e['archivo']} v{e['version']}: el manifiesto dice {e['filas']:,} filas "
            f"y el CSV trae {len(pdf):,}"
        )

    return pdf.assign(_archivo=e["archivo"], _version=e["version"], _sha256=e["sha256"])


def carga(archivo: str, manifiesto: list[dict], lakehouse: str) -> None:
    """Deja en su tabla las versiones de `archivo` que falten. Idempotente.

    El manifiesto trae los dos archivos revueltos y se acota antes de comparar: cada uno
    tiene su tabla, y `pendientes` compara contra una sola.
    """
    tabla = TABLAS[archivo]
    ruta = ruta_tabla(tabla, lakehouse)
    suyas = [e for e in manifiesto if e["archivo"] == archivo]
    falta = pendientes(ruta, suyas, LLAVES)

    # El estado estable es no hacer nada: el archivo sólo cambia una vez al año.
    if not falta:
        apunta(tabla, pendientes=0, de=len(suyas), filas=0)
        return

    trozos = [baja(e) for e in falta]
    escribe(a_spark(pd.concat(trozos, ignore_index=True), {"_version": LongType()}), ruta)

    leidas = {(e["archivo"], e["version"]): len(t) for e, t in zip(falta, trozos)}
    reconcilia(ruta, leidas, LLAVES)
    apunta(
        tabla,
        pendientes=len(falta),
        de=len(suyas),
        filas=sum(leidas.values()),
        version=version_de(ruta),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

manifiesto = manifiesto_de(FUENTE)

for archivo in TABLAS:
    carga(archivo, manifiesto, "lh_bronze")

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
