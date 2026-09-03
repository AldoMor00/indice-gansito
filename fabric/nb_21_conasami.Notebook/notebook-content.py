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

from pyspark.sql import Window  # sólo este notebook lo usa: el cierre de vigencias

# Silver de CONASAMI: tipa los dos archivos y resuelve qué salario regía cada quincena. Va
# aparte de nb_20 porque su fuente se mueve una vez al año y no cuelga del lote de precios.
# Mismo trato que allá: tres tiempos —armar, validar, escribir—, ANSI para el tipado y nada
# aterriza si una compuerta truena (decisiones #15 y #16).
#
# Dos tablas, una por archivo, porque los dos CSV no comparten una sola columna:
#   dim_salario_minimo   SCD2 sobre `salario_zonas`: el salario por zona y su vigencia
#   dim_salario_mensual  la serie mensual tipada, de donde gold saca el deflactor
#
# `de_bronze`, `clave`, `upsert` y las compuertas vienen de nb_00_config.

# La vigencia abierta se cierra con centinela y no con nulo: `es_vigente` sale de comparar
# contra ella, y el BETWEEN de gold no tiene que arrastrar un `OR IS NULL`.
ABIERTA = "9999-12-31"

# Los 7 literales de `zona_salarial` en las 42 filas del archivo. Un octavo es otro renombre
# —como el de 2025— y partiría una zona en dos historias sin que nadie lo vea: se decide.
LITERALES = ["a", "b", "c", "unica", "resto del pais", "general", "zlfn"]

# De las 89 columnas se tipan tres. Los 86 salarios profesionales por oficio no responden
# ninguna pregunta del proyecto y además traen celdas vacías: ni entran ni se exigen.
LLAVE_ZONA = ["inicio_vigencia", "zona_salarial"]
COLUMNAS_INDICE = ["anio", "mes", "smg_nominal", "smg_real", "smgr_indice"]


def ultima_version(filas):
    """Bronze conserva todas las versiones del archivo porque no deduplica; elegir es de
    silver. Espeja `ultimo_intento` de nb_20 sobre la otra columna de linaje: esta fuente
    no tiene período, se versiona por sha256 (decisión #9)."""
    maximos = filas.groupBy("_archivo").agg(F.max("_version").alias("_version"))
    return filas.join(maximos, ["_archivo", "_version"])


def calendario_de_vigencias(zonas):
    """Una fila por `inicio_vigencia` distinto, cerrada el día antes del siguiente.

    El cierre es global y no por zona porque CONASAMI reexpide el tabulador completo: una
    vigencia termina cuando entra uno nuevo, no cuando esa zona cambia de salario. Partido
    por zona, `a`, `b`, `c` y `unica` quedarían abiertas para siempre —su último renglón no
    tiene siguiente— y una quincena de 2024 haría match con las seis.
    """
    orden = Window.orderBy("vigencia_desde")  # sin partición: el calendario es una sola serie
    return (
        zonas.select("inicio_vigencia")
        .distinct()
        .withColumn("vigencia_desde", F.to_date("inicio_vigencia"))
        .withColumn(
            "vigencia_hasta",
            F.coalesce(
                F.date_sub(F.lead("vigencia_desde").over(orden), 1),
                F.lit(ABIERTA).cast("date"),
            ),
        )
    )


def exige_literales_conocidos(zonas) -> None:
    """Truena si aparece un `zona_salarial` fuera de LITERALES. Un renombre no visto no lo
    ve ningún cast: da una zona nueva con la historia partida a la mitad."""
    nuevos = (
        zonas.filter(~F.col("zona_salarial").isin(LITERALES))
        .select("zona_salarial")
        .distinct()
        .collect()
    )
    if nuevos:
        raise RuntimeError(
            "`zona_salarial` desconocida — " + ", ".join(f["zona_salarial"] for f in nuevos)
        )


def exige_vigencia_continua(dim) -> None:
    """Truena si una zona tiene un hueco entre dos de sus vigencias.

    Es el punto ciego del cierre global: si una zona sale de un tabulador y vuelve en el
    siguiente, su renglón anterior se cerró el día que entró aquel donde no está, y entre
    ese día y su regreso no hay salario. Las tres veces que una zona salió del archivo —`c`
    en 2012-11-27, `a` y `b` en 2015-10-01, `unica` en 2019-01-01— fue fusión y ninguna
    volvió. Si alguna vuelve, qué significa se decide a mano.
    """
    siguiente = F.lead("vigencia_desde").over(
        Window.partitionBy("zona").orderBy("vigencia_desde")
    )
    huecos = (
        dim.withColumn("_siguiente", siguiente)
        # `date_sub` sobre el siguiente y no `date_add` sobre el cierre: el centinela de la
        # vigencia abierta se desbordaría del rango de `date`.
        .filter(
            F.col("_siguiente").isNotNull()
            & (F.col("vigencia_hasta") != F.date_sub("_siguiente", 1))
        )
        .collect()
    )
    if huecos:
        raise RuntimeError(
            "vigencias con hueco — "
            + "; ".join(f"{f['zona']}: {f['vigencia_hasta']} → {f['_siguiente']}" for f in huecos)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Las dos tablas son independientes y no comparten lote, pero las dos escrituras van al
# final: una compuerta que truena no deja media dimensión puesta. El estado estable es no
# hacer nada —la fuente se mueve una vez al año— y se ve en el resumen como un MERGE de 0
# insertadas y 0 actualizadas, que además no commitea versión.

zonas_bronze = de_bronze("salario_zonas")
zonas = ultima_version(zonas_bronze)

exige_completo(zonas, LLAVE_ZONA + ["salario_minimo_general"])
exige_literales_conocidos(zonas)
exige_uno_por_clave(zonas, LLAVE_ZONA, ["salario_minimo_general"])

por_zona = zonas.withColumn(
    # `resto del pais` (2019-2024) y `general` (2025-) son la misma zona renombrada, así que
    # la identidad va sobre el nombre normalizado y el literal queda de atributo, que es lo
    # que la SCD2 versiona. `unica` (2015-2018) no se fusiona con ellas: al crearse la ZLFN
    # en 2019 se le recortó territorio, y eso es cambio real, no cambio de nombre.
    "zona",
    F.when(F.col("zona_salarial") == "resto del pais", F.lit("general")).otherwise(
        F.col("zona_salarial")
    ),
)

vigencias = calendario_de_vigencias(zonas)
dim_salario_minimo = por_zona.join(vigencias, "inicio_vigencia").select(
    # La clave natural del renglón es (zona, inicio_vigencia): la SCD2 guarda una versión por
    # vigencia, no una fila por zona.
    clave("zona", "inicio_vigencia").alias("id_salario_zona"),
    "zona",
    F.col("zona_salarial").alias("zona_literal"),
    F.col("salario_minimo_general").cast("decimal(10,2)").alias("salario_minimo_general"),
    "vigencia_desde",
    "vigencia_hasta",
    (F.col("vigencia_hasta") == F.lit(ABIERTA).cast("date")).alias("es_vigente"),
)

apunta(
    "bronze_zonas",
    filas=zonas_bronze.count(),
    vigencias=vigencias.count(),
    zonas=por_zona.select("zona").distinct().count(),
)

indice_bronze = de_bronze("salario_indice")
indice = ultima_version(indice_bronze)

exige_completo(indice, COLUMNAS_INDICE)
exige_uno_por_clave(indice, ["anio", "mes"], ["smg_nominal", "smg_real", "smgr_indice"])

dim_salario_mensual = indice.select(
    clave("anio", "mes").alias("id_mes"),
    # `mes_inicio` es conversión, no derivación: la misma etiqueta en un tipo con el que se
    # puede unir. Gold pega aquí el `quincena_inicio` del hecho truncado al mes, porque el
    # deflactor es mensual y las dos quincenas de un mes comparten el suyo (fuentes.md).
    F.to_date(F.concat_ws("-", "anio", F.lpad("mes", 2, "0"), F.lit("01"))).alias("mes_inicio"),
    F.col("anio").cast("int").alias("anio"),
    F.col("mes").cast("int").alias("mes"),
    # decimal(10,4): el nominal va de 0.0242 en 1969 a 324.75 en 2026. El deflactor
    # —`smg_nominal / smg_real`, el INPC entre 100 (fuentes.md)— no se materializa: es la
    # división de dos columnas que ya están aquí, y eso es de gold (decisión #12).
    F.col("smg_nominal").cast("decimal(10,4)").alias("smg_nominal"),
    F.col("smg_real").cast("decimal(10,2)").alias("smg_real"),
    F.col("smgr_indice").cast("decimal(10,2)").alias("smgr_indice"),
)

apunta("bronze_indice", filas=indice_bronze.count(), meses=dim_salario_mensual.count())

# Compuertas de salida: sólo lo que existe después de transformar.
exige_llave_unica(dim_salario_minimo, "id_salario_zona")
exige_vigencia_continua(dim_salario_minimo)
exige_llave_unica(dim_salario_mensual, "id_mes")

# El MERGE genérico basta para la SCD2 porque la fuente reexpide la historia completa: la
# vigencia se deriva del lote, así que el renglón abierto del año pasado vuelve a llegar con
# su `vigencia_hasta` ya cerrado y `whenMatchedUpdateAll` lo actualiza. Nada que expirar a
# mano ni un update aparte (decisión #14).
upsert(dim_salario_minimo, "dim_salario_minimo", ["id_salario_zona"])
upsert(dim_salario_mensual, "dim_salario_mensual", ["id_mes"])

# Predicados de una fila: lo único que Delta sabe expresar. `smg_real` es el divisor del
# deflactor en gold, como `gramos` lo es de `precio_por_gramo`.
exige_invariantes(
    ruta_tabla("dim_salario_minimo", SILVER),
    {
        "salario_positivo": "salario_minimo_general > 0",
        "vigencia_ordenada": "vigencia_hasta >= vigencia_desde",
    },
)
exige_invariantes(
    ruta_tabla("dim_salario_mensual", SILVER),
    {"smg_real_positivo": "smg_real > 0"},
)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
