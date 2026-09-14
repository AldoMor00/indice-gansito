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

# Mantenimiento de las tablas Delta de las tres capas: compacta, aplica el layout de la
# decisión #32 y borra los archivos que ya nadie referencia. Lo dispara `pl_mantenimiento`
# una vez por semana, y antes del refresh del modelo y del re-clon de dev, que son las otras
# dos actividades de ese pipeline (decisión #33).
#
# Va en notebook y no en la *Lakehouse maintenance activity* del pipeline porque esa no
# soporta lakehouses con esquemas, y los tres lo son. Aquí no corre nada: la corrida es la
# celda de abajo.
#
# El guard de lakehouse por defecto, `ruta_tabla`, las capas, `CORRIDA` y el trío del
# resumen vienen de nb_00_config.

# El perfil, explícito como en los notebooks de escritura: con High concurrency la sesión se
# comparte y el perfil del que corrió antes seguiría puesto. Éste reescribe archivos, así que
# le importa cuál es.
spark.conf.set("spark.fabric.resourceProfile", "writeHeavy")

# `VORDER` sólo donde lee Direct Lake. No sirve para ponerlo hacia atrás —de eso se encarga la
# escritura de nb_30— pero sin él el `OPTIMIZE` se lo quitaría a lo que reescriba: esta sesión
# corre en `writeHeavy`, que lo trae apagado, y la sesión le gana a la propiedad de tabla.
CAPAS = {BRONZE: "", SILVER: "", GOLD: " VORDER"}

# Siete días, escritos. Es el mismo número que el default, pero escrito queda la intención en
# el código; bajarlo exigiría apagar `retentionDurationCheck`, y lo que se gana es ver borrar
# antes a cambio de que un lector en curso pierda los archivos que está usando.
RETENCION_HORAS = 168


def tablas_de(capa: str) -> list[str]:
    """Las tablas de una capa, enumeradas en vivo. Una lista escrita aquí envejecería: lo que
    se mantiene es lo que haya, no lo que había el día que se escribió esto.

    Una capa sin `Tables/dbo` —prod todavía no tiene gold— da cero y no truena. Mantener lo
    que no existe es no hacer nada, y eso no es un fallo.
    """
    ruta = ruta_tabla("", capa)
    if not notebookutils.fs.exists(ruta):
        return []
    return sorted(f.name for f in notebookutils.fs.ls(ruta))


def compacta(ruta: str, vorder: str) -> dict:
    """`OPTIMIZE`, y lo que hay que saber de él: cuánto reescribió y, si la tabla está
    clusterizada, qué tan bien quedó el layout.

    `clusteringQuality` viene dentro de las métricas en PySpark —la doc lo presenta como un
    método exclusivo de Scala—, así que la calidad del layout sale sin una llamada aparte
    (`hechos.md`). Se reporta el solapamiento peor de las columnas de cluster: 0 es que los
    rangos no se pisan, que es lo ideal, y es lo que se degrada cuando entra dato nuevo.
    """
    metricas = spark.sql(f"OPTIMIZE delta.`{ruta}`{vorder}").first()["metrics"].asDict()
    medido = {
        "agregados": metricas["numFilesAdded"],
        "quitados": metricas["numFilesRemoved"],
    }
    calidad = metricas.get("clusteringQuality")
    if calidad:
        medido["solapamiento"] = round(max(c["overlapRatio"] for c in calidad), 4)
    return medido


def limpia(ruta: str) -> int:
    """`VACUUM`, y cuántos archivos de datos huérfanos se llevó.

    El conteo sale de un `DRY RUN` previo porque el `VACUUM` a secas no lo devuelve, y un
    borrado que no dice cuánto borró no se puede vigilar. Cuesta listar el directorio dos
    veces, que a este volumen no se nota.

    Del conteo se descuenta el `metadata/` que Fabric mantiene por tabla —la virtualización
    Iceberg de OneLake—: el `DRY RUN` lo lista como candidato en **todas** las tablas, así que
    sin descontarlo cada corrida reportaría un borrado de más. El `VACUUM` no lo borra, es un
    directorio y no un archivo, pero el reporte no tiene por qué heredar esa confusión.

    `VACUUM ... LITE` no lo listaría siquiera, porque arma la lista desde el log en vez del
    directorio. No se usa: aquí ignoró también huérfanos de verdad que el modo completo sí
    ve, y recuperar espacio es para lo que está esto (`hechos.md`).
    """
    consulta = f"VACUUM delta.`{ruta}` RETAIN {RETENCION_HORAS} HOURS"
    candidatos = [f["path"] for f in spark.sql(f"{consulta} DRY RUN").collect()]
    spark.sql(consulta)
    return sum(1 for p in candidatos if not p.rstrip("/").endswith("/metadata"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# `OPTIMIZE` antes que `VACUUM` y en la misma pasada por tabla: el primero deja huérfanos los
# archivos que compactó y el segundo se los lleva si ya pasaron la retención. Al revés, el
# `VACUUM` correría antes de que existan los huérfanos que iba a limpiar.
#
# Lo que rompe el clon de dev es justamente esa pareja (decisión #5), y por eso el re-clon es
# la última actividad del pipeline y no algo que este notebook pueda resolver.
for capa, vorder in CAPAS.items():
    tablas = tablas_de(capa)
    apunta(capa, tablas=len(tablas))
    for tabla in tablas:
        ruta = ruta_tabla(tabla, capa)
        medido = compacta(ruta, vorder)
        medido["huerfanos"] = limpia(ruta)
        apunta(f"{capa}.{tabla}", **medido)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
