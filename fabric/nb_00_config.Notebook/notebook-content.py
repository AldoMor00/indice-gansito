# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Lo que comparten los notebooks de bronze. Se trae con `%run nb_00_config`, no con una
# wheel: ver decisión #7. Aquí sólo entra lo que tiene más de un consumidor; lo que
# cambia entre fuentes —dónde viven los archivos y cómo se bajan— se queda en cada uno.

import io
import json

import pandas as pd
import requests
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Confirmar que no hay Default Lakehouse
_runt_ctx = notebookutils.runtime.context
if _runt_ctx["defaultLakehouseId"] is not None:
    raise RuntimeError(
        f"""Lakehouse por defecto enlazado: ({_runt_ctx['defaultLakehouseName']}).
        Los notebooks no deben llevar ningún default lakehouse. Retirar desde la UI.
        Motivo: De tenerlo, al desplegar a prod el notebook apuntará al lakehouse de dev.
        """
    )

CORRIDA = _runt_ctx["activityId"]
RAW = "https://raw.githubusercontent.com/AldoMor00/indice-gansito-datos/main"


# El resumen de la corrida: un solo lugar por donde sale un número. El `print` se queda en
# el snapshot del notebook; lo que el pipeline recibe y puede encadenar es el exit value,
# así que todo lo que importe pasa por aquí y sale al final.
RESUMEN = {}


def apunta(paso: str, **datos) -> None:
    """Al log del notebook y al resumen que se devuelve, de una sola escritura."""
    RESUMEN[paso] = datos
    legible = ", ".join(
        # `type` y no `isinstance`: un bool es int en Python y saldría como 1.
        f"{k}={v:,}" if type(v) is int else f"{k}={v}"
        for k, v in datos.items()
    )
    print(f"{paso:<18}: {legible}")


def termina() -> None:
    """Último renglón del notebook: `exit` corta la ejecución, así que nada va después.
    El pipeline lo lee en @activity('<notebook>').output.result.exitValue."""
    notebookutils.notebook.exit(json.dumps({"corrida": CORRIDA, **RESUMEN}, ensure_ascii=False))


def version_de(ruta: str) -> int:
    """La versión que esta corrida dejó en la tabla. Es lo que empata el resumen con
    DESCRIBE HISTORY, que ya es la bitácora de escrituras y no hay que duplicar."""
    return DeltaTable.forPath(spark, ruta).history(1).first()["version"]


def ruta_tabla(tabla: str, lakehouse: str, workspace_id: str | None = None) -> str:
    """Ruta OneLake de una tabla. GUIDs en los dos segmentos: mezclarlos con nombres da
    400, y los nombres se renombran. Nada hardcodeado: se pide por nombre y se resuelve
    en vivo, así el mismo código corre en dev y en prod.

    Sin `workspace_id` es el workspace de la corrida, que es lo que quieren nb_10 y
    nb_11. Lo pasa nb_91_clona_bronze, el único que mira a otro workspace.
    """
    ws = workspace_id or notebookutils.runtime.context["currentWorkspaceId"]
    lh = notebookutils.lakehouse.get(lakehouse, ws)["id"]
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables/dbo/{tabla}"


def manifiesto_de(fuente: str) -> list[dict]:
    """El manifiesto de una fuente. Es el índice: el repo no lista directorio."""
    r = requests.get(f"{RAW}/{fuente}/manifiesto.jsonl", timeout=60)
    r.raise_for_status()
    return [json.loads(l) for l in r.text.splitlines() if l.strip()]


def pendientes(ruta: str, manifiesto: list[dict], llaves: list[tuple[str, str]]) -> list[dict]:
    """Entradas del manifiesto que la tabla todavía no tiene. Todas, si aún no existe.

    `llaves` empareja cada campo del manifiesto con su columna de linaje, porque las
    fuentes no se llavean igual: Profeco por (quincena, intento) y CONASAMI por
    (archivo, version).
    """
    if not DeltaTable.isDeltaTable(spark, ruta):
        return list(manifiesto)
    cols = [c for _, c in llaves]
    ya = {
        tuple(f[c] for c in cols)
        for f in spark.read.format("delta").load(ruta).select(*cols).distinct().collect()
    }
    return [e for e in manifiesto if tuple(e[k] for k, _ in llaves) not in ya]


def a_spark(pdf: pd.DataFrame, tipos: dict):
    """A Spark con esquema explícito: inferir es castear, y bronze no castea.

    El esquema sale de pdf.columns para que el orden case con el de las tuplas:
    itertuples entrega por posición y una columna corrida no daría error, sólo datos
    mal puestos. `tipos` es la excepción, para las columnas de linaje que no vienen de
    la fuente sino de nosotros; todo lo demás es texto.
    """
    esquema = StructType(
        [StructField(c, tipos.get(c, StringType()), True) for c in pdf.columns]
    )
    return spark.createDataFrame(
        list(pdf.itertuples(index=False, name=None)), schema=esquema
    ).withColumns({
        "_ingestado_utc": F.current_timestamp(),
        "_corrida": F.lit(CORRIDA),
    })


def reconcilia(ruta: str, leidas: dict, llaves: list[tuple[str, str]]) -> None:
    """Lo que quedó en la tabla contra lo que se bajó. Un descuadre es pipeline roto,
    no dato malo, así que truena."""
    cols = [c for _, c in llaves]
    real = {
        tuple(f[c] for c in cols): f["n"]
        for f in spark.read.format("delta").load(ruta)
        .groupBy(*cols).count()
        .withColumnRenamed("count", "n").collect()
    }
    descuadres = {k: (v, real.get(k, 0)) for k, v in leidas.items() if v != real.get(k, 0)}
    if descuadres:
        raise RuntimeError(f"descuadre bajado vs. tabla — {descuadres}")


def escribe(sdf, ruta: str) -> None:
    """Append con `mergeSchema` apagado a propósito: el esquema de las dos fuentes está
    medido en docs/fuentes.md, así que una columna nueva es alarma de lote, no algo que
    se absorba en silencio."""
    sdf.write.format("delta").mode("append").option("mergeSchema", "false").save(ruta)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
