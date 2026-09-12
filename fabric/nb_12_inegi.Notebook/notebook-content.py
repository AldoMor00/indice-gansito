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

# Bronze de INEGI: el INPC quincenal, que es el deflactor del índice real (decisión #35).
# Misma regla que las otras dos fuentes —no castea, no filtra, no deduplica— y la misma
# llave que CONASAMI: (archivo, version), porque tampoco tiene período y se versiona por
# sha256 (decisión #9). Aquí no corre nada.

# Explícito aunque `writeHeavy` sea el default del workspace: con High concurrency los
# notebooks de un pipeline comparten sesión, y el perfil del que corrió antes seguiría
# puesto aquí. Cada notebook declara el suyo.
spark.conf.set("spark.fabric.resourceProfile", "writeHeavy")

FUENTE = "inpc"

LLAVES = [("archivo", "_archivo"), ("version", "_version")]

TABLA = "inpc_quincenal"


def url_de(e: dict) -> str:
    """Espeja ruta() de scripts/ingesta_inpc.py. Una versión > 1 lleva sufijo."""
    sufijo = "" if e["version"] == 1 else f"_v{e['version']}"
    return f"{RAW}/{FUENTE}/serie/{e['archivo']}{sufijo}.json"


def baja(e: dict) -> pd.DataFrame:
    """Las observaciones del JSON que sirvió la API, ya con su linaje pegado.

    El JSON viene anidado —una serie con sus observaciones dentro— y desanidarlo no es
    filtrar: la observación es la fila. De los atributos de la serie suben dos, `FREQ` y
    `UNIT`, que son los que dicen que sigue siendo el nivel del índice y sigue siendo
    quincenal; silver los comprueba antes de deflactar nada.

    Sin `dtype`: la API entrega cada valor como cadena, así que pandas no infiere y bronze
    no castea. Con `dtype=str` los nulos de `OBS_EXCEPTION` llegarían como la cadena 'None'.
    """
    serie = requests.get(url_de(e), timeout=60).json()["Series"][0]
    pdf = pd.DataFrame(serie["OBSERVATIONS"])

    # Contra el manifiesto, antes de escribir nada.
    if len(pdf) != e["observaciones"]:
        raise RuntimeError(
            f"{e['archivo']} v{e['version']}: el manifiesto dice {e['observaciones']:,} "
            f"observaciones y el JSON trae {len(pdf):,}"
        )

    return pdf.assign(
        _archivo=e["archivo"],
        _version=e["version"],
        _sha256=e["sha256"],
        _freq=serie["FREQ"],
        _unit=serie["UNIT"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

manifiesto = manifiesto_de(FUENTE)
ruta = ruta_tabla(TABLA, BRONZE)
falta = pendientes(ruta, manifiesto, LLAVES)

if not falta:
    # El estado estable es no hacer nada: la serie se baja cada quincena y sólo entra una
    # versión nueva cuando el sha256 cambia.
    apunta(TABLA, pendientes=0, de=len(manifiesto), filas=0)
else:
    trozos = [baja(e) for e in falta]
    escribe(a_spark(pd.concat(trozos, ignore_index=True), {"_version": LongType()}), ruta)

    leidas = {(e["archivo"], e["version"]): len(t) for e, t in zip(falta, trozos)}
    reconcilia(ruta, leidas, LLAVES)
    apunta(
        TABLA,
        pendientes=len(falta),
        de=len(manifiesto),
        filas=sum(leidas.values()),
        version=version_de(ruta),
    )

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
