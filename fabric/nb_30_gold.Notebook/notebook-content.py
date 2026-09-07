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

# PARAMETERS CELL ********************

# Qué quincenas recalcula la corrida. Vacío —el 99%— lo deriva del estado de gold. Con
# valores fuerza esas quincenas aunque silver no las haya tocado: un cambio de reglas del
# índice no mueve el linaje de silver, así que la comparación de estados no puede verlo.
# `todas` es el uso normal de esa rama y evita transcribir las 46.
#
# Cadena y no lista, por la misma razón que en nb_20: los base parameters de la actividad
# de notebook sólo llevan escalares.
quincenas_pedidas = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold: el modelo en estrella que lee Direct Lake. Proyecta silver —no revalida lo que sus
# compuertas ya garantizaron (decisión #16)— y agrega lo único que silver no da: los
# relativos pareados de los que sale el índice encadenado (decisión #19).
#
# Siete tablas. Tres hechos y cuatro dimensiones; `dim_mes` concilia los dos granos de
# tiempo —el precio es quincenal y el salario mensual— como shrunken conformed dimension,
# así que ninguno de los dos hechos se desnaturaliza para caber en el otro.
#
# `ruta_tabla`, `clave`, `upsert`, `exige_llave_unica` y el trío del resumen vienen de
# nb_00_config. `upsert` recibe `GOLD` explícito: su default es silver, de donde salió.

# `hechos_precios` es el estado de gold, igual que en silver: qué quincenas ya se
# proyectaron. Si no existe —primera corrida— todo sale pendiente y el backfill es esta misma.
TABLA_HECHOS = "hechos_precios"
TABLA_RELATIVOS = "hechos_relativos"

# El canal colapsa el `giro` que ya declara Profeco, en vez de mapear las 57 cadenas a mano:
# una cadena nueva llega con su giro puesto y un diccionario de cadenas se rompería en la
# quincena siguiente. Es la granularidad que sobrevive el umbral de pareo (decisión #19).
#
# Son los cinco giros que venden la canasta —los mismos para los 9 SKUs que para el Gansito
# solo—; el universo de tiendas tiene 15 y los otros 10 no venden esto y se quedan sin canal.
# Un giro nuevo que sí traiga precio detiene la corrida.
CANAL = {
    "Supermercado / Tienda de Autoservicio": "supermercado",
    "Tienda de Conveniencia": "conveniencia",
    "Farmacias": "conveniencia",
    "Mercados": "tradicional",
    "Central de Abasto": "tradicional",
}


def de_silver(tabla: str):
    """Lee una tabla de silver. Espeja `de_bronze` de nb_00_config."""
    return spark.read.format("delta").load(ruta_tabla(tabla, SILVER))


def orden_de(columna: str):
    """Ordinal global de una quincena `yyyy-MM_qN`: 24 por año, 2 por mes, +1 la segunda.

    Se deriva de la etiqueta y no de un `row_number` sobre el lote: el ordinal tiene que
    significar lo mismo en la corrida que trae una quincena y en la que trae las 46, porque
    es lo que define quién es la quincena anterior.
    """
    anio = F.substring(columna, 1, 4).cast("int")
    mes = F.substring(columna, 6, 2).cast("int")
    segunda = F.when(F.col(columna).endswith("q2"), F.lit(1)).otherwise(F.lit(0))
    return anio * 24 + (mes - 1) * 2 + segunda


def exige_sin_huecos(quincenas: list[str]) -> None:
    """Truena si el calendario de quincenas tiene un hueco.

    Es la compuerta propia de gold y no un lujo: el eslabón une una quincena con la de
    ordinal inmediatamente anterior, así que un hueco no produce un eslabón equivocado
    —produce **ninguno**—, y la cadena se partiría en silencio dejando el índice plano en
    ese tramo. El resto de las compuertas ya las pasó silver.
    """
    ordenes = sorted(
        f["_orden"]
        for f in spark.createDataFrame([(q,) for q in quincenas], "_quincena string")
        .select(orden_de("_quincena").alias("_orden"))
        .collect()
    )
    huecos = [
        (a, b) for a, b in zip(ordenes, ordenes[1:]) if b != a + 1
    ]
    if huecos:
        raise RuntimeError(f"el calendario de quincenas tiene huecos — ordinales {huecos}")


def a_recalcular(todas: list[str], parametro: str) -> list[str]:
    """Qué quincenas recorre esta corrida: el parámetro si lo hay, y si no el estado.

    Espeja `a_recalcular` de nb_20. Una quincena mal escrita dejaría el lote vacío y la
    corrida saldría no-op y verde, que es el fallback callado que no se tolera.
    """
    pedidas = [q.strip() for q in parametro.split(",") if q.strip()]
    if not pedidas:
        return pendientes_gold(todas)

    if pedidas == ["todas"]:
        return todas

    desconocidas = set(pedidas) - set(todas)
    if desconocidas:
        raise RuntimeError(f"quincenas que silver no tiene — {sorted(desconocidas)}")
    return sorted(pedidas)


def pendientes_gold(todas: list[str]) -> list[str]:
    """Las quincenas que silver tiene y gold no."""
    ruta = ruta_tabla(TABLA_HECHOS, GOLD)
    if not DeltaTable.isDeltaTable(spark, ruta):
        return todas

    ya = {
        f["_quincena"]
        for f in spark.read.format("delta").load(ruta).select("_quincena").distinct().collect()
    }
    return [q for q in todas if q not in ya]


def reemplaza_quincenas(nuevas, tabla: str, quincenas: list[str]) -> None:
    """Hecho: se reescriben las particiones de las quincenas recalculadas y nada más.
    Mismo patrón que en silver —la quincena está completa o no está— pero contra gold.

    Delta valida que lo escrito caiga dentro del predicado, así que una fila de otra
    quincena truena en vez de colarse.
    """
    if not quincenas:
        apunta(tabla, filas=0, quincenas=0)
        return

    filtro = "_quincena IN (" + ", ".join(f"'{q}'" for q in quincenas) + ")"
    (
        nuevas.write.format("delta")
        .mode("overwrite")
        .partitionBy("_quincena")
        .option("replaceWhere", filtro)
        .save(ruta_tabla(tabla, GOLD))
    )
    apunta(tabla, filas=nuevas.count(), quincenas=len(quincenas))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tres tiempos, como en silver: armar, validar, escribir. Nada llama `.write` hasta el
# final, así que los DataFrames son la etapa de staging y una compuerta que truena no deja
# nada a medias.

precios_silver = de_silver(TABLA_HECHOS)
todas = sorted(
    f["_quincena"] for f in precios_silver.select("_quincena").distinct().collect()
)
exige_sin_huecos(todas)

quincenas_lote = a_recalcular(todas, quincenas_pedidas)
apunta("lote", quincenas=len(quincenas_lote), de_silver=len(todas))

# El eslabón necesita la quincena anterior, así que el lote de lectura es más ancho que el
# de escritura: se leen las pedidas y sus predecesoras, y nada más. La primera de la serie
# no tiene predecesora y por eso no produce eslabón —el índice arranca en 100 ahí—.
posicion = {q: i for i, q in enumerate(todas)}
previas = {todas[posicion[q] - 1] for q in quincenas_lote if posicion[q] > 0}
necesarias = sorted(set(quincenas_lote) | previas)

# ---------------------------------------------------------------- dimensiones

# Las dimensiones salen de silver completo y no del lote: son acumulativas y caben de sobra,
# y acotarlas al lote dejaría al hecho apuntando a filas que todavía no existen.
tiendas_silver = de_silver("dim_tienda")

# La compuerta se acota a los giros que el hecho alcanza, no a los 15 de la dimensión:
# `dim_tienda` es el universo completo del archivo y no el de las tiendas que venden la
# canasta (decisión #2), así que trae zapaterías y papelerías que nunca van a tener precio.
# Exigirles canal sería pedirle a la fuente que clasifique lo que no estamos midiendo.
giros_con_precio = {
    f["giro"]
    for f in precios_silver.select("id_tienda")
    .distinct()
    .join(tiendas_silver.select("id_tienda", "giro"), "id_tienda")
    .select("giro")
    .distinct()
    .collect()
}
sin_canal = giros_con_precio - CANAL.keys()
if sin_canal:
    raise RuntimeError(f"`giro` con precio y sin canal en CANAL — {sorted(sin_canal)}")

dim_tienda = tiendas_silver.withColumn(
    "canal",
    # `giro` se queda tal cual para los cortes transversales, que no necesitan pareo y
    # donde las cinco categorías valen; `canal` es sólo para el índice.
    #
    # Cadena de `when` sin `otherwise` y no un `create_map[...]`: la tienda fuera de la
    # canasta cae en nulo, que es lo correcto —no tiene canal porque no vende esto— y
    # además con ANSI encendido el lookup de un mapa con llave ausente truena en vez de
    # dar nulo. `coalesce` se queda con el primer `when` que acierta.
    F.coalesce(*[F.when(F.col("giro") == g, F.lit(c)) for g, c in CANAL.items()]),
)

dim_producto = de_silver("dim_producto")

# `dim_mes` es la parte de calendario de la serie salarial: mismo grano, misma llave. No se
# recalcula `clave("anio", "mes")` aquí —silver la hasheó sobre las columnas crudas del CSV,
# donde enero es "1" y no "01"— sino que se hereda, y las quincenas la encuentran por el par
# de enteros. Rehashearlo sería inventar una llave que no empata con nada.
salario_silver = de_silver("hechos_salario_mensual")
dim_mes = salario_silver.select("id_mes", "mes_inicio", "anio", "mes")

hechos_salario_mensual = salario_silver.select(
    "id_mes",
    "smg_nominal",
    "smg_real",
    "smgr_indice",
    # Aquí sí se materializa: es la división que silver dejó pendiente a propósito porque
    # es derivable de dos columnas que ya están, y esa división es de gold (decisión #12).
    (F.col("smg_nominal") / F.col("smg_real")).cast("decimal(12,6)").alias("inpc"),
)

# El calendario quincenal: 46 filas, con su ordinal y su mes. El `id_mes` se resuelve por
# join contra dim_mes y no recalculando la llave, por lo dicho arriba.
quincenas = (
    spark.createDataFrame([(q,) for q in todas], "_quincena string")
    .select(
        clave("_quincena").alias("id_quincena"),
        F.col("_quincena").alias("quincena"),
        F.to_date(
            F.concat(
                F.substring("_quincena", 1, 7),
                F.when(F.col("_quincena").endswith("q1"), "-01").otherwise("-16"),
            )
        ).alias("quincena_inicio"),
        orden_de("_quincena").alias("orden"),
        F.substring("_quincena", 1, 4).cast("int").alias("anio"),
        F.substring("_quincena", 6, 2).cast("int").alias("mes"),
    )
)
dim_tiempo_quincena = quincenas.join(dim_mes.select("id_mes", "anio", "mes"), ["anio", "mes"])

# Un `join` que pierde filas dejaría quincenas sin mes y el deflactor mudo justo ahí. Es
# posible de verdad: CONASAMI publica una vez al año y la ventana de precios podría
# adelantarse a la del salario.
if dim_tiempo_quincena.count() != len(todas):
    faltan = quincenas.join(dim_mes.select("anio", "mes"), ["anio", "mes"], "left_anti")
    raise RuntimeError(
        "quincenas sin mes en la serie salarial — "
        + ", ".join(f["quincena"] for f in faltan.collect())
    )

# ---------------------------------------------------------------- hechos

base = (
    precios_silver.filter(F.col("_quincena").isin(necesarias))
    .select("id_tienda", "id_producto", "_quincena", "precio_promedio")
    .withColumn("_orden", orden_de("_quincena"))
)

hechos_precios = (
    precios_silver.filter(F.col("_quincena").isin(quincenas_lote))
    .select(
        clave("_quincena").alias("id_quincena"),
        "id_tienda",
        "id_producto",
        "precio_promedio",
        "observaciones",
        "precio_min",
        "precio_max",
        # Linaje y predicado de `replaceWhere` a la vez: la etiqueta legible es lo que se lee
        # en el log y lo que acota qué particiones tiene derecho a pisar esta corrida.
        "_quincena",
    )
)

# El eslabón lo arma `eslabones`, de nb_00_config, para que nb_90 pruebe esta misma función
# y no una copia suya.
#
# El filtro final no sobra: `base` trae las predecesoras además de las pedidas, así que con un
# lote no contiguo —pongamos q5 y q7— el join produciría también el eslabón de q6, que no es
# de esta corrida. `replaceWhere` valida que lo escrito caiga en su predicado, así que esa
# fila no se colaría: tronaría la escritura entera.
hechos_relativos = eslabones(base).filter(F.col("_quincena").isin(quincenas_lote)).select(
    # `F.col` y no la cadena pelada: `clave` pasa a `xxhash64`, que interpretaría
    # "_quincena" bien, pero el hábito de pasar Columns evita el problema del alias.
    clave(F.col("_quincena")).alias("id_quincena"),
    clave(F.col("_quincena_anterior")).alias("id_quincena_anterior"),
    "id_tienda",
    "id_producto",
    "log_relativo",
    "_quincena",
)

# ---------------------------------------------------------------- compuertas de salida

# Las llaves nuevas de gold. Las de silver ya se revisaron allá y no se repiten: aquí sólo
# van las que este notebook inventa.
exige_llave_unica(dim_tiempo_quincena, "id_quincena")
exige_llave_unica(dim_mes, "id_mes")

apunta(
    "relativos",
    eslabones=len(quincenas_lote) - (1 if todas and todas[0] in quincenas_lote else 0),
    pares=hechos_relativos.count(),
)

# ---------------------------------------------------------------- escritura

# Las dimensiones antes que el hecho, por lo mismo que en silver: `hechos_precios` es el
# punto de commit del que `pendientes_gold` lee el estado, así que una corrida que muriera
# entre medias deja dimensiones de más —que el upsert vuelve a poner igual— y la quincena
# todavía pendiente.
upsert(dim_producto, "dim_producto", ["id_producto"], GOLD)
upsert(dim_tienda, "dim_tienda", ["id_tienda"], GOLD)
upsert(dim_mes, "dim_mes", ["id_mes"], GOLD)
upsert(dim_tiempo_quincena, "dim_tiempo_quincena", ["id_quincena"], GOLD)
upsert(hechos_salario_mensual, "hechos_salario_mensual", ["id_mes"], GOLD)
reemplaza_quincenas(hechos_relativos, TABLA_RELATIVOS, quincenas_lote)
reemplaza_quincenas(hechos_precios, TABLA_HECHOS, quincenas_lote)

# `smg_real` es el divisor del deflactor y `precio_promedio` el del relativo: un cero castea
# perfecto y ANSI no lo ve. Son predicados de una fila, que es lo único que Delta expresa.
exige_invariantes(
    ruta_tabla("hechos_salario_mensual", GOLD), {"smg_real_positivo": "smg_real > 0"}
)
exige_invariantes(
    ruta_tabla(TABLA_HECHOS, GOLD), {"precio_promedio_positivo": "precio_promedio > 0"}
)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
