# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

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

manifiesto = [
    json.loads(l)
    for l in requests.get(f"{RAW}/manifiesto.jsonl", timeout=60).text.splitlines()
    if l.strip()
]


def ruta_tabla(tabla: str, lakehouse: str) -> str:
    """Ruta OneLake de una tabla. GUIDs en los dos segmentos: mezclarlos con nombres da
    400, y los nombres se renombran. Nada hardcodeado: se pide por nombre y se resuelve
    en vivo, así el mismo código corre en dev y en prod."""
    ws = notebookutils.runtime.context["currentWorkspaceId"]
    lh = notebookutils.lakehouse.get(lakehouse)["id"]
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables/dbo/{tabla}"


def url_de(e: dict, zona: str) -> str:
    """Espeja rutas() de scripts/ingesta.py. Un intento > 1 lleva sufijo."""
    sufijo = "" if e["intento"] == 1 else f"_i{e['intento']}"
    prefijo = "qqp" if zona == "precios" else "tiendas"
    return f"{RAW}/{zona}/anio={e['quincena'][:4]}/{prefijo}_{e['quincena']}{sufijo}.parquet"


def pendientes(ruta: str) -> list[dict]:
    """Entradas del manifiesto que la tabla todavía no tiene. Todas, si aún no existe."""
    if not DeltaTable.isDeltaTable(spark, ruta):
        return list(manifiesto)
    ya = {
        (f["_quincena"], f["_intento"])
        for f in spark.read.format("delta").load(ruta)
        .select("_quincena", "_intento").distinct().collect()
    }
    return [e for e in manifiesto if (e["quincena"], e["intento"]) not in ya]


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


def a_spark(pdf: pd.DataFrame):
    """A Spark con esquema explícito: inferir es castear, y bronze no castea.

    El esquema sale de pdf.columns para que el orden case con el de las tuplas:
    itertuples entrega por posición y una columna corrida no daría error, sólo datos
    mal puestos. Todo es texto salvo _intento, que no viene de la fuente: ese lo pongo yo.
    """
    tipos = {"_intento": LongType()}
    esquema = StructType(
        [StructField(c, tipos.get(c, StringType()), True) for c in pdf.columns]
    )
    return spark.createDataFrame(
        list(pdf.itertuples(index=False, name=None)), schema=esquema
    ).withColumns({
        "_ingestado_utc": F.current_timestamp(),
        "_corrida": F.lit(CORRIDA),
    })


def reconcilia(ruta: str, leidas: dict) -> None:
    """Lo que quedó en la tabla contra lo que se bajó. Un descuadre es pipeline roto,
    no dato malo, así que truena."""
    real = {
        (f["_quincena"], f["_intento"]): f["n"]
        for f in spark.read.format("delta").load(ruta)
        .groupBy("_quincena", "_intento").count()
        .withColumnRenamed("count", "n").collect()
    }
    descuadres = {k: (v, real.get(k, 0)) for k, v in leidas.items() if v != real.get(k, 0)}
    if descuadres:
        raise RuntimeError(f"descuadre bajado vs. tabla — {descuadres}")


def carga(zona: str, tabla: str, lakehouse: str) -> None:
    """Deja en `tabla` las quincenas del manifiesto que falten. Idempotente."""
    ruta = ruta_tabla(tabla, lakehouse)
    falta = pendientes(ruta)
    print(f"{zona}: {len(falta)} pendientes de {len(manifiesto)}")

    # El estado estable del cron es no hacer nada. No es un fallo.
    if not falta:
        return

    trozos = [baja(e, zona) for e in falta]
    (a_spark(pd.concat(trozos, ignore_index=True))
        .write.format("delta").mode("append").option("mergeSchema", "false").save(ruta))

    leidas = {(e["quincena"], e["intento"]): len(t) for e, t in zip(falta, trozos)}
    reconcilia(ruta, leidas)
    print(f"{zona}: {sum(leidas.values()):,} filas cargadas y reconciliadas")


carga("precios", "zz_prueba_precios", "lh_silver")
carga("tiendas", "zz_prueba_tiendas", "lh_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
