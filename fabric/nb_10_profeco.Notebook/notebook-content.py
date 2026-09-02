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

# Bronze de Profeco: deja en Delta los parquets de indice-gansito-datos, sin castear,
# filtrar ni deduplicar (regla dura #2). Lo compartido con nb_11_conasami vive en
# nb_00_config. Aquí no corre nada: la corrida es la celda de abajo.

FUENTE = "profeco"

# Cómo se llavea esta fuente: campo del manifiesto -> columna de linaje.
LLAVES = [("quincena", "_quincena"), ("intento", "_intento")]


def url_de(e: dict, zona: str) -> str:
    """Espeja rutas() de scripts/ingesta_profeco.py. Un intento > 1 lleva sufijo."""
    sufijo = "" if e["intento"] == 1 else f"_i{e['intento']}"
    prefijo = "qqp" if zona == "precios" else "tiendas"
    return (
        f"{RAW}/{FUENTE}/{zona}/anio={e['quincena'][:4]}/"
        f"{prefijo}_{e['quincena']}{sufijo}.parquet"
    )


def baja(e: dict, zona: str) -> pd.DataFrame:
    """Un parquet del repo de datos, ya con su linaje pegado.

    Se lee en el driver con pandas: a escala real sería un cuello de botella y esto
    lo leería Spark. Se justifica porque la zona raw entera pesa 6.5 MB.
    """
    pdf = pd.read_parquet(io.BytesIO(requests.get(url_de(e, zona), timeout=60).content))

    # Contra el manifiesto, antes de escribir nada. Sólo precios: de tiendas no se
    # guardó conteo, así que esa sólo se puede verificar contra lo que se bajó.
    if zona == "precios" and len(pdf) != e["filas_filtradas"]:
        raise RuntimeError(
            f"{e['quincena']}: el manifiesto dice {e['filas_filtradas']:,} filas "
            f"y el parquet trae {len(pdf):,}"
        )

    return pdf.assign(_quincena=e["quincena"], _intento=e["intento"], _sha256=e["sha256"])


def carga(zona: str, manifiesto: list[dict], lakehouse: str) -> None:
    """Deja en la tabla `zona` las quincenas del manifiesto que falten.

    `zona` nombra tres cosas a la vez —el directorio del repo de datos, el prefijo del
    parquet y la tabla de bronze— y se llaman igual a propósito: un argumento aparte para
    la tabla sólo serviría para escribir bronze en otro lado, que es justo lo que no debe
    poder pasarse por accidente.
    """
    ruta = ruta_tabla(zona, lakehouse)
    falta = pendientes(ruta, manifiesto, LLAVES)

    # El estado estable del cron es no hacer nada, y no es un fallo. Se apunta igual: el
    # cero explícito es lo único que hace visible el no-op.
    if not falta:
        apunta(zona, pendientes=0, de=len(manifiesto), filas=0)
        return

    trozos = [baja(e, zona) for e in falta]
    escribe(a_spark(pd.concat(trozos, ignore_index=True), {"_intento": LongType()}), ruta)

    leidas = {(e["quincena"], e["intento"]): len(t) for e, t in zip(falta, trozos)}
    reconcilia(ruta, leidas, LLAVES)
    apunta(
        zona,
        pendientes=len(falta),
        de=len(manifiesto),
        filas=sum(leidas.values()),
        version=version_de(ruta),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# El manifiesto es el índice de la zona raw —el repo no lista directorio— y el estado
# contra el que se decide qué falta. Las dos zonas lo comparten, pero cada una tiene su
# tabla y su conteo, así que van por separado.

manifiesto = manifiesto_de(FUENTE)

carga("precios", manifiesto, "lh_bronze")
carga("tiendas", manifiesto, "lh_bronze")

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
