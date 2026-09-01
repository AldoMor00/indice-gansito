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
# filtrar ni deduplicar. Lo compartido con nb_11_conasami vive en nb_00_config.

FUENTE = "profeco"

# Cómo se llavea esta fuente: campo del manifiesto -> columna de linaje.
LLAVES = [("quincena", "_quincena"), ("intento", "_intento")]

manifiesto = manifiesto_de(FUENTE)


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


def carga(zona: str, lakehouse: str) -> None:
    """Deja en la tabla `zona` las quincenas del manifiesto que falten. Idempotente."""
    ruta = ruta_tabla(zona, lakehouse)
    falta = pendientes(ruta, manifiesto, LLAVES)
    print(f"{zona}: {len(falta)} pendientes de {len(manifiesto)}")

    # El estado estable del cron es no hacer nada. No es un fallo.
    if not falta:
        return

    trozos = [baja(e, zona) for e in falta]
    escribe(a_spark(pd.concat(trozos, ignore_index=True), {"_intento": LongType()}), ruta)

    leidas = {(e["quincena"], e["intento"]): len(t) for e, t in zip(falta, trozos)}
    reconcilia(ruta, leidas, LLAVES)
    print(f"{zona}: {sum(leidas.values()):,} filas cargadas y reconciliadas")


carga("precios", "lh_bronze")
carga("tiendas", "lh_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
