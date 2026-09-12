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

# Silver de INEGI: tipa el INPC quincenal y le pone la llave de la quincena, que es el
# grano con el que gold lo pega al hecho de precios (decisión #35). Va aparte de nb_20 por
# lo mismo que nb_21: su fuente no cuelga del lote de precios. Tres tiempos —armar,
# validar, escribir—, ANSI para el tipado y nada aterriza si una compuerta truena
# (decisiones #15 y #16).
#
# Una tabla:
#   hechos_inpc_quincenal   el nivel del índice por quincena, de donde sale el deflactor
#
# `de_bronze`, `clave`, `upsert` y las compuertas vienen de nb_00_config.

spark.conf.set("spark.fabric.resourceProfile", "readHeavyForSpark")

# Los dos atributos que vuelven a esta serie usable como deflactor: `15` es quincenal y
# `1051` es "Índice base 2018=100". Se comprueban porque casi todos los indicadores vecinos
# de 910420 son variación porcentual y no nivel —ver scripts/ingesta_inpc.py—: si INEGI
# moviera el id, el deflactor pasaría a valer ~0.3 en vez de ~145 y ni el cast ni el
# `inpc > 0` lo verían.
FREQ_QUINCENAL, UNIT_INDICE = "15", "1051"

OBLIGATORIAS = ["TIME_PERIOD", "OBS_VALUE"]

# `AAAA/MM/0Q`, con el ordinal de la quincena en el tercer campo. De aquí cuelga el
# `substring` de `quincena_de`.
PERIODO = r"^\d{4}/\d{2}/0[12]$"


def ultima_version(filas):
    """Bronze conserva todas las versiones del archivo porque no deduplica; elegir es de
    silver. Igual que en nb_21: esta fuente no tiene período, se versiona por sha256
    (decisión #9)."""
    maximos = filas.groupBy("_archivo").agg(F.max("_version").alias("_version"))
    return filas.join(maximos, ["_archivo", "_version"])


def quincena_de(periodo):
    """`2026/08/02` a `2026-08_q2`, la etiqueta con la que se llavea todo el proyecto.

    El tercer campo de INEGI es el ordinal de la quincena dentro del mes, no el día: `02`
    es la segunda quincena y no el día 2. Se toma sólo su último carácter porque la
    etiqueta del proyecto es `q2` y no `q02`; que el campo sea siempre `01` o `02` lo
    garantiza `exige_periodo_quincenal`, no este `substring`.
    """
    return F.concat(
        F.substring(periodo, 1, 4),
        F.lit("-"),
        F.substring(periodo, 6, 2),
        F.lit("_q"),
        F.substring(periodo, 10, 1),
    )


def exige_serie_conocida(obs) -> None:
    """Truena si la serie dejó de ser el nivel quincenal del índice.

    Es lo que un cast no puede ver: una variación porcentual castea igual de bien que un
    nivel, entra positiva y sale como un deflactor que divide por casi nada.
    """
    atributos = {
        (f["_freq"], f["_unit"]) for f in obs.select("_freq", "_unit").distinct().collect()
    }
    if atributos != {(FREQ_QUINCENAL, UNIT_INDICE)}:
        raise RuntimeError(
            f"la serie dejó de ser el nivel quincenal del índice — esperado FREQ/UNIT "
            f"('{FREQ_QUINCENAL}', '{UNIT_INDICE}'), llegó {sorted(atributos)}"
        )


def exige_periodo_quincenal(obs, muestra: int = 4) -> None:
    """Truena si un `TIME_PERIOD` no tiene la forma `AAAA/MM/0Q`.

    Un formato distinto no daría error: daría una etiqueta de quincena silenciosamente
    equivocada, que en gold es una llave que no empata y un eslabón perdido.
    """
    rotas = obs.filter(~F.col("TIME_PERIOD").rlike(PERIODO))
    if rotas.take(1):
        raise RuntimeError(
            f"`TIME_PERIOD` fuera de formato en {rotas.count():,} filas — "
            + muestra_filas(rotas, ["TIME_PERIOD", "OBS_VALUE"], muestra)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# La serie entera, sin recortar a la ventana de precios: silver tipa lo que la fuente da y
# es gold quien toma las quincenas que necesita. Mismo trato que la serie mensual de
# CONASAMI, que también empieza mucho antes que el panel.

inpc_bronze = de_bronze("inpc_quincenal")
obs = ultima_version(inpc_bronze)

# Compuertas de entrada: lo que es de la fuente, antes de derivar nada.
exige_completo(obs, OBLIGATORIAS)
exige_serie_conocida(obs)
exige_periodo_quincenal(obs)

hechos_inpc_quincenal = obs.withColumn("_quincena", quincena_de("TIME_PERIOD")).select(
    clave("_quincena").alias("id_quincena"),
    "_quincena",
    # decimal(12,6), la misma escala que tenía el deflactor implícito de CONASAMI al que
    # sustituye. La ventana del índice entra exacta —de 2024 en adelante INEGI publica tres
    # decimales— y lo que se redondea es la cola rebaseada de antes de 2000, donde la peor
    # pérdida es 5e-7 sobre un nivel de ~25.
    F.col("OBS_VALUE").cast("decimal(12,6)").alias("inpc"),
)

apunta("bronze_inpc", filas=inpc_bronze.count(), quincenas=hechos_inpc_quincenal.count())

# Compuerta de salida: sólo lo que existe después de transformar.
exige_llave_unica(hechos_inpc_quincenal, "id_quincena")

upsert(hechos_inpc_quincenal, "hechos_inpc_quincenal", ["id_quincena"])

# Predicado de una fila: lo único que Delta sabe expresar. El INPC es el divisor del índice
# real, igual que `smg_real` lo es del deflactor que reemplaza.
exige_invariantes(
    ruta_tabla("hechos_inpc_quincenal", SILVER),
    {"inpc_positivo": "inpc > 0"},
)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
