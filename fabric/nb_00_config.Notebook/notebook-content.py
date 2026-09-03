# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Lo que comparten los notebooks. Se trae con `%run nb_00_config`, no con una wheel: ver
# decisión #7. Aquí sólo entra lo que tiene más de un consumidor; lo que cambia entre
# fuentes —dónde viven los archivos y cómo se bajan— se queda en cada uno.

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
BRONZE, SILVER = "lh_bronze", "lh_silver"

# ANSI encendido. Fabric lo trae apagado, así que un `cast` fallido daría nulo en silencio;
# con esto truena. Es lo que vuelve a `cast` y `try_cast` dos decisiones distintas y
# visibles —"esto tiene que pasar" contra "esto puede faltar"— en vez de la misma escrita de
# dos formas, y lo que hace que el tipado no necesite compuerta propia (decisión #15).
spark.conf.set("spark.sql.ansi.enabled", "true")


# El resumen de la corrida: un solo lugar por donde sale un número. El `print` se queda en
# el snapshot del notebook; lo que el pipeline recibe y puede encadenar es el exit value,
# así que todo lo que importe pasa por aquí y sale al final.
RESUMEN = {}


def apunta(paso: str, **datos) -> None:
    """Al log del notebook y al resumen que se devuelve, de una sola escritura.

    El paso repetido truena. Antes se pisaba: el log mostraba las dos líneas y el exit
    value salía con una, sin avisar, que es justo el fallback callado que no se tolera.
    """
    if paso in RESUMEN:
        raise RuntimeError(f"`{paso}` ya está en el resumen: dos apunta() con el mismo nombre")
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


# Las compuertas de silver. Corren antes de escribir: lo que no pasa no aterriza, y el
# arreglo es el notebook y no la tabla (decisión #15). Con ANSI encendido el casteo truena
# solo, así que aquí sólo va lo que un cast no puede ver.


def exige_completo(sdf, columnas: list[str]) -> None:
    """Truena si una columna obligatoria viene vacía. Es el hueco que ANSI no tapa: el nulo
    castea a nulo sin protestar, y las llaves naturales ni siquiera se castean —se hashean—.

    La cadena vacía cuenta como faltante. Los CSV se leen con `keep_default_na=False` para
    que bronze no castee (decisión #9), así que una celda vacía de CONASAMI llega como "" y
    no como nulo; los parquets de Profeco sí traen nulo. Misma noticia, dos formas. Es para
    las columnas de texto de bronze.
    """
    conteos = sdf.agg(*[
        F.count_if(F.col(c).isNull() | (F.trim(F.col(c)) == "")).alias(c) for c in columnas
    ]).first()
    vacias = {c: n for c, n in zip(columnas, conteos) if n}
    if vacias:
        raise RuntimeError(
            "columnas obligatorias vacías — "
            + "; ".join(f"{c}: {n:,} filas" for c, n in vacias.items())
        )


def exige_uno_por_clave(sdf, llaves: list[str], atributos: list[str], muestra: int = 3) -> None:
    """Truena si un atributo trae más de un valor bajo la misma clave natural.

    Antes esto lo resolvía un `max_by` que se quedaba con el más reciente, callado. Que un
    atributo cambie bajo su clave es la fuente cambiando —o la clave dejando de identificar—
    y las dos cosas se miran antes de escribir, no se desempatan solas.

    Los conflictos se cuentan sólo si los hay: en la corrida sana esto es una pasada y nada.
    """
    conflictos = (
        sdf.groupBy(*llaves)
        .agg(*[F.count_distinct(c).alias(c) for c in atributos])
        .filter(" OR ".join(f"`{c}` > 1" for c in atributos))
    )
    ejemplos = conflictos.take(muestra)
    if ejemplos:
        raise RuntimeError(
            f"{conflictos.count():,} claves con un atributo cambiado debajo — "
            + "; ".join(str(f.asDict()) for f in ejemplos)
        )


def exige_invariantes(ruta: str, checks: dict[str, str]) -> None:
    """Deja puestas las constraints CHECK de la tabla, sin repetir las que ya están.

    Las aplica el notebook y no un DDL suelto porque una constraint puesta a mano desaparece
    al recrear la tabla, y una protección que crees tener y no tienes es peor que ninguna.

    Reparto: la constraint se queda con lo que es predicado de **una fila del resultado** —es
    lo único que Delta sabe expresar, sin agregados ni subconsultas— y las compuertas de
    arriba con todo lo que necesita ver la fuente, el lote o varias filas. Así nada se valida
    dos veces (decisión #15).

    En la corrida que crea la tabla el `ALTER` va después de escribir, así que un dato que
    viole el invariante deja la tabla escrita y truena al ponerle la constraint: se dropea y
    se vuelve a correr. De la segunda en adelante lo rechazado es la escritura misma.
    """
    puestas = {
        k.rsplit(".", 1)[-1]
        for k in DeltaTable.forPath(spark, ruta).detail().first()["properties"]
        if k.startswith("delta.constraints.")
    }
    for nombre, predicado in checks.items():
        if nombre not in puestas:
            spark.sql(f"ALTER TABLE delta.`{ruta}` ADD CONSTRAINT {nombre} CHECK ({predicado})")
            print(f"constraint {nombre}: puesta")


def exige_llave_unica(sdf, llave: str) -> None:
    """Truena si la llave sustituta se repite. La dimensión ya viene agrupada por su clave
    natural, así que un duplicado aquí sólo puede ser una colisión de `xxhash64`, y una
    colisión fusiona dos filas distintas sin dejar rastro: el hash no propaga nulos ni
    avisa, devuelve una llave válida y equivocada."""
    filas, distintas = sdf.count(), sdf.select(llave).distinct().count()
    if filas != distintas:
        raise RuntimeError(f"{llave}: {filas:,} filas y {distintas:,} llaves distintas")


def de_bronze(tabla: str):
    """Lee la tabla delta de bronze."""
    return spark.read.format("delta").load(ruta_tabla(tabla, BRONZE))


def clave(*cols):
    """Clave sustituta determinista sobre la clave natural. Es lo que deja que el MERGE
    junte por un `bigint` en vez de por un par de cadenas largas, y que la clave se
    calcule sin consultar la dimensión: no hace falta un paso previo que reparta ids.
    Silver se recalcula sola y los ids no pueden cambiar bajo gold."""
    return F.xxhash64(*cols)


def upsert(nuevas, tabla: str, llaves: list[str]) -> None:
    """Dimensión: MERGE por clave. Inserta lo nuevo y actualiza sólo lo que de verdad
    cambió —de ahí la condición sobre los atributos—, para que la corrida sin novedades
    no reescriba un solo archivo. No se sobrescribe la tabla porque una dimensión es
    acumulativa: una tienda que salió del panel no deja de existir, y los hechos
    históricos la siguen apuntando.

    Un MERGE que no cambia nada no commitea versión, así que el rastro se lee comparando
    la versión de antes contra la de después y no mirando la última entrada del log, que
    en ese caso sería de otra operación.
    """
    ruta = ruta_tabla(tabla, SILVER)
    if not DeltaTable.isDeltaTable(spark, ruta):
        filas = nuevas.count()
        # Crear la dimensión vacía es un estado roto que se lee como éxito. Pasa si se dropea
        # la dimensión sin dropear el hecho —entonces no hay quincenas pendientes y el lote
        # viene vacío— y también si bronze está vacío.
        if not filas:
            raise RuntimeError(f"{tabla} no existe y el lote viene vacío: no hay qué crear")
        nuevas.write.format("delta").save(ruta)
        apunta(tabla, creada=True, insertadas=filas, actualizadas=0)
        return

    dt = DeltaTable.forPath(spark, ruta)
    antes = dt.history(1).first()["version"]
    atributos = [c for c in nuevas.columns if c not in llaves]
    (
        dt.alias("d")
        # Condición de join: "d.id_producto = n.id_producto". Con varias llaves, unidas
        # por AND. `d` es lo que ya está en la tabla, `n` lo que trae esta corrida.
        .merge(nuevas.alias("n"), " AND ".join(f"d.{k} = n.{k}" for k in llaves))
        # Condición de update: "NOT (d.marca <=> n.marca) OR NOT (d.gramos <=> n.gramos)".
        # O sea, actualiza sólo si algún atributo difiere. `<=>` es igualdad nula-segura:
        # `null <=> null` da cierto, `null = null` daría null y el cambio pasaría de largo.
        .whenMatchedUpdateAll(" OR ".join(f"NOT (d.{c} <=> n.{c})" for c in atributos))
        .whenNotMatchedInsertAll()
        .execute()
    )

    despues = DeltaTable.forPath(spark, ruta).history(1).first()
    if despues["version"] == antes:
        apunta(tabla, insertadas=0, actualizadas=0, version=antes)
        return

    metricas = despues["operationMetrics"]
    apunta(
        tabla,
        insertadas=int(metricas["numTargetRowsInserted"]),
        actualizadas=int(metricas["numTargetRowsUpdated"]),
        version=despues["version"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
