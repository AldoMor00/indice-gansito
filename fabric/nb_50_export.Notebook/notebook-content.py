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

# La copia de gold que vive fuera de Fabric: las ocho tablas como un parquet plano cada una
# en `Files/publico`, de donde se commitean al repo de datos y las lee el modelo import de
# la cuenta pública (decisión #6).
#
# Es la única salida del proyecto que va en dirección contraria. Todo lo demás lee de git y
# escribe a OneLake; esto lee de OneLake para escribir a git, y existe porque la capacidad es
# una trial que va a desaparecer con `lh_gold` (decisión #1). El reporte público no puede
# quedarse colgado de ella.
#
# Plano y no Delta, y un archivo por tabla y no 46: el conector Web de Power BI lee un
# parquet por URL, no un directorio con su `_delta_log`. Y `overwrite`, que las capas del
# medallón nunca usan, aquí es lo correcto: esto no es una tabla con historia sino una foto
# completa de gold, y quien lleva la historia es git.

# El orden es el de lectura del modelo: primero las dimensiones, luego los hechos.
TABLAS = [
    "dim_producto",
    "dim_tienda",
    "dim_tiempo_quincena",
    "dim_mes",
    "hechos_precios",
    "hechos_relativos",
    "hechos_ic_indice",
    "hechos_salario_mensual",
]

DESTINO = "publico"


def ruta_files(carpeta: str, lakehouse: str) -> str:
    """Espejo de `ruta_tabla` sobre `Files`. Se queda aquí y no en nb_00_config porque es el
    único notebook que escribe fuera de `Tables`. Absoluta y no relativa: la ruta relativa
    resuelve contra el lakehouse por defecto, que estos notebooks no tienen (decisión #3)."""
    ws = notebookutils.runtime.context["currentWorkspaceId"]
    lh = notebookutils.lakehouse.get(lakehouse, ws)["id"]
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Files/{carpeta}"


def exporta(tabla: str, destino: str) -> dict:
    """Una tabla de gold a un parquet plano con nombre estable.

    Spark escribe un directorio con su `part-0000-<uuid>.parquet` adentro, así que hay que
    sacar el archivo y tirar el directorio: la URL que consume Power BI tiene que ser fija y
    el `uuid` cambia en cada corrida.

    El `coalesce(1)` no es sólo para que salga uno solo. Con las 46 particiones de
    `hechos_precios` en un archivo el diccionario comprime sobre la columna entera, así que
    el resultado pesa menos que la suma de las partes.
    """
    gold = spark.read.format("delta").load(ruta_tabla(tabla, GOLD))
    filas = gold.count()

    temporal = f"{destino}/_{tabla}"
    gold.coalesce(1).write.mode("overwrite").parquet(temporal)

    parte = next(a for a in notebookutils.fs.ls(temporal) if a.name.endswith(".parquet"))
    # `create_path` explícito: en notebooks Spark viene en False y la carpeta ya está creada.
    notebookutils.fs.mv(parte.path, f"{destino}/{tabla}.parquet", create_path=False, overwrite=True)
    notebookutils.fs.rm(temporal, recurse=True)

    # El tamaño se lee de la parte y no del destino: `ls` de un archivo suelto no está
    # documentado, y aquí ya lo tenemos medido.
    return {"filas": filas, "bytes": parte.size}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Se exporta siempre y completo: son ~2 MB, y una foto parcial de gold no le sirve a nadie.
# Quien decide si la corrida aportó algo es el diff de git al commitear, no el notebook.
destino = ruta_files(DESTINO, GOLD)
notebookutils.fs.mkdirs(destino)

for tabla in TABLAS:
    apunta(tabla, **exporta(tabla, destino))

# El total es lo que se compara contra el techo de GitHub y contra la corrida anterior: si
# crece de golpe, cambió el grano de alguna tabla y no el dato.
apunta(
    "publico",
    tablas=len(TABLAS),
    filas=sum(RESUMEN[t]["filas"] for t in TABLAS),
    bytes=sum(RESUMEN[t]["bytes"] for t in TABLAS),
)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
