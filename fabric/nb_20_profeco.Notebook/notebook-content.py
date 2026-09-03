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

# Silver de Profeco: tipa, resuelve identidad y agrega a quincena. Lee de bronze el
# intento vigente de cada quincena y recalcula sólo lo pendiente, así que el backfill y
# la corrida del cron son el mismo código. Decisiones #12 y #13.
#
# El guard de lakehouse por defecto, `ruta_tabla`, `CORRIDA`, `DeltaTable` y el trío del
# resumen —`apunta`, `termina`, `version_de`— vienen de nb_00_config. CONASAMI va aparte,
# en nb_21: su dimensión se mueve una vez al año y no cuelga de este lote.

BRONZE, SILVER = "lh_bronze", "lh_silver"

# `hechos_precios` es el estado de silver: qué quincenas ya se procesaron y con qué
# intento. Si no existe —primera corrida— todo sale pendiente y el backfill es esta misma.
TABLA_HECHOS = "hechos_precios"

# La canasta son 9 SKUs, no 11: las Barritas Fresa y Piña salen de toda la serie porque
# Profeco las reclasificó a Galletas Dulces y desaparecen del corte en 2025-03_q2. La
# exclusión va aquí y no en objetivo.yml, donde comparten `producto` con el resto
# (decisión #13).
EXCLUIDOS = [
    "Paquete con 2 Barritas. Fresa (67 Gr.)",
    "Paquete con 2 Barritas. Piña (67 Gr.)",
]


def de_bronze(tabla: str):
    """Lee la tabla delta de bronze."""
    return spark.read.format("delta").load(ruta_tabla(tabla, BRONZE))


def clave(*cols):
    """Clave sustituta determinista sobre la clave natural. Es lo que deja que el MERGE
    junte por un `bigint` en vez de por un par de cadenas largas, y que la clave se
    calcule sin consultar la dimensión: no hace falta un paso previo que reparta ids.
    Silver se recalcula sola y los ids no pueden cambiar bajo gold."""
    return F.xxhash64(*cols)


def ultimo_intento(filas):
    """Un `intento` > 1 es una quincena rebajada: gana el mayor. Bronze conserva los dos
    porque no deduplica; elegir es de silver. Sirve para `precios` y para `tiendas`: las
    dos llevan el mismo linaje."""
    maximos = filas.groupBy("_quincena").agg(F.max("_intento").alias("_intento"))
    return filas.join(maximos, ["_quincena", "_intento"])


def pendientes_silver(precios) -> list[str]:
    """Las quincenas que silver no tiene, o que bronze rebajó con un intento mayor.
    Espeja pendientes() de nb_00_config, que hace lo mismo contra el manifiesto."""
    vigentes = precios.select("_quincena", "_intento").distinct()
    ruta = ruta_tabla(TABLA_HECHOS, SILVER)
    if not DeltaTable.isDeltaTable(spark, ruta):
        return [fila["_quincena"] for fila in vigentes.collect()]

    ya = spark.read.format("delta").load(ruta).select("_quincena", "_intento").distinct()
    faltan = vigentes.join(ya, ["_quincena", "_intento"], "left_anti")
    return [fila["_quincena"] for fila in faltan.collect()]


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
        nuevas.write.format("delta").save(ruta)
        apunta(tabla, creada=True, insertadas=nuevas.count(), actualizadas=0)
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


def reemplaza_quincenas(nuevas, tabla: str, quincenas: list[str]) -> None:
    """Hecho: se reescriben las particiones de las quincenas recalculadas y nada más.
    `replaceWhere` hace la corrida re-ejecutable sin duplicar y cuesta lo que pesan esas
    quincenas, no lo que pesa la tabla —la diferencia que importa cuando el hecho no cabe
    en memoria—. Un MERGE daría el mismo resultado leyendo la tabla entera para buscar
    filas que por construcción no existen: la quincena está completa o no está.

    Delta valida que lo escrito caiga dentro del predicado, así que una fila de otra
    quincena truena en vez de colarse.
    """
    if not quincenas:
        apunta(tabla, filas=0, quincenas=0)
        return

    # "_quincena IN ('2024-01_q1', '2024-01_q2', ...)" — el predicado de las particiones
    # que esta corrida tiene derecho a pisar.
    filtro = "_quincena IN (" + ", ".join(f"'{q}'" for q in quincenas) + ")"
    (
        nuevas.write.format("delta")
        .mode("overwrite")
        .partitionBy("_quincena")
        .option("replaceWhere", filtro)
        .save(ruta_tabla(tabla, SILVER))
    )
    apunta(tabla, filas=nuevas.count(), quincenas=len(quincenas))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# `canasta` son las 46 quincenas al intento vigente sin los SKUs excluidos; `lote` es
# sólo lo que falta recalcular. Las dimensiones se escriben antes que el hecho a
# propósito: el hecho es el punto de commit. Si algo truena a media corrida, la dimensión
# quedó con filas de más —que el upsert vuelve a poner igual— y la quincena sigue
# pendiente.

bronze_precios = de_bronze("precios")
canasta = (
    ultimo_intento(bronze_precios)
    .filter(~F.col("presentacion").isin(EXCLUIDOS))
    .cache()
)

quincenas_pendientes = sorted(pendientes_silver(canasta))
lote = canasta.filter(F.col("_quincena").isin(quincenas_pendientes))

apunta("bronze_precios", filas=bronze_precios.count())
apunta("canasta", filas=canasta.count(), excluidos=len(EXCLUIDOS))
apunta("pendiente", quincenas=len(quincenas_pendientes), filas=lote.count())

# Con el lote vacío —el estado estable del cron— todo lo que sigue es un no-op: el MERGE
# no encuentra nada que insertar y no commitea versión. No hace falta cortar aquí, y un
# solo punto de salida se lee mejor.

# Los dos formatos de `presentacion` y lo que saca cada regex:
#   "Paquete con 6 Mantecadas. Vainilla (188 Gr.)"  ->  piezas=6,    gramos=188
#   "Paquete 280 Gr. Panqué Nuez"                   ->  piezas=nada, gramos=280
# Que los 9 SKUs parseen es regla de nb_40_dq, no un assert aquí.
PIEZAS = r"Paquete con (\d+)\s"     # "Paquete con", el número, y el espacio que lo cierra
GRAMOS = r"(\d+(?:\.\d+)?) Gr\."    # el número —con decimales opcionales— antes de " Gr."

dim_producto = (
    lote.groupBy("presentacion", "marca")
    # `producto` y `categoria` son constantes bajo la clave en las 46 quincenas, pero el
    # MERGE truena si el origen trae dos filas con el mismo id. `max_by` desempata por
    # quincena más reciente; que alguna vez haya habido dos valores lo reporta nb_40_dq.
    .agg(
        F.max_by("producto", "_quincena").alias("producto"),
        F.max_by("categoria", "_quincena").alias("categoria"),
    )
    .select(
        clave("presentacion", "marca").alias("id_producto"),
        "marca",
        "presentacion",
        F.coalesce(
            F.regexp_extract("presentacion", PIEZAS, 1).try_cast("int"),
            F.lit(1),  # "Paquete 280 Gr. Panqué Nuez" no declara piezas: es uno
        ).alias("piezas"),
        # `gramos` y no `precio_por_gramo`: el precio vive en el hecho, así que la
        # división es de gold. Materializarla aquí sería guardar una columna derivable de
        # otras dos, justo lo que la decisión #12 descartó para la bandera booleana.
        F.regexp_extract("presentacion", GRAMOS, 1).try_cast("decimal(7,2)").alias("gramos"),
        "producto",
        "categoria",
    )
)

upsert(dim_producto, "dim_producto", ["id_producto"])

# Las tiendas salen del corte completo del archivo, no del de precios: si salieran de ahí
# la dimensión quedaría sesgada a las que venden pastelillos (decisión #2). Por eso este
# bloque no lee `lote` sino su propia tabla, acotada a las mismas quincenas pendientes.
bronze_tiendas = de_bronze("tiendas")
tiendas_lote = (
    ultimo_intento(bronze_tiendas)
    .filter(F.col("_quincena").isin(quincenas_pendientes))
)

apunta("bronze_tiendas", filas=bronze_tiendas.count())

# La clave de una tienda es `(nombre_comercial, direccion)` y es la única: búsqueda
# exhaustiva de los 255 subconjuntos de los 8 campos sobre las 46 quincenas. `direccion`
# es la del inmueble —Sears y Liverpool comparten la de la plaza— y `nombre_comercial`
# distingue al inquilino; ninguno alcanza solo (decisión #13).
LLAVE_TIENDA = ["nombre_comercial", "direccion"]
ATRIBUTOS_TIENDA = ["cadena_comercial", "giro", "estado", "municipio", "latitud", "longitud"]

dim_tienda = (
    tiendas_lote.groupBy(*LLAVE_TIENDA)
    # Bajo la clave no cambia un solo atributo en 46 quincenas, así que dim_tienda no
    # lleva SCD2: no hay nada que versionar. El `max_by` no está por si cambian, sino
    # porque el MERGE truena si el origen trae dos filas con el mismo id; si algún día
    # difieren gana la quincena más reciente y nb_40_dq lo reporta.
    .agg(*[F.max_by(col, "_quincena").alias(col) for col in ATRIBUTOS_TIENDA])
    .select(
        clave(*LLAVE_TIENDA).alias("id_tienda"),
        "nombre_comercial",
        "direccion",
        "cadena_comercial",
        "giro",
        "estado",
        "municipio",
        # Decimal y no double: la coordenada no identifica —el 10.1% de las filas comparte
        # una, porque Profeco geocodifica el mercado y no el local— pero es exacta y no se
        # mueve en 46 quincenas. Es atributo geográfico para gold, y un cast fallido deja
        # nulo y lo reporta nb_40_dq: el conteo de inválidas es del hecho (decisión #15),
        # y una coordenada ilegible no invalida a la tienda.
        F.col("latitud").try_cast("decimal(9,6)").alias("latitud"),
        F.col("longitud").try_cast("decimal(9,6)").alias("longitud"),
    )
)

upsert(dim_tienda, "dim_tienda", ["id_tienda"])

# El tipado. `precio` es lo único que se castea a número en el camino del hecho, así que
# es lo único que puede resultar inválido: `fecha_registro` viene en `yyyy/MM/dd` sin
# ambigüedad y las llaves son texto que no se convierte.
con_precio = lote.withColumns({
    "id_tienda": clave("nombre_comercial", "direccion"),
    "id_producto": clave("presentacion", "marca"),
    # `try_cast` explícito y no `cast`: Fabric deja ANSI apagado, así que un cast fallido
    # daría null de todos modos. Silver elige en vez de heredar.
    "precio_tipado": F.col("precio").try_cast("decimal(10,2)"),
})

# El grano es tienda-SKU-quincena, no la visita: Profeco visita la misma tienda hasta
# cinco veces por quincena y `fecha_registro` no trae hora, así que las capturas del mismo
# día no se pueden ordenar. Promediar conserva las dos observaciones en vez de escoger una
# sin criterio, y `observaciones`, `precio_min` y `precio_max` dejan ver si el número se
# observó de verdad sin una bandera derivable (decisión #12).
#
# Se agrega sobre `con_precio` y no sobre las válidas, para que la celda cuyas
# observaciones fallaron todas exista con `precio_promedio` nulo en vez de desaparecer
# (decisión #15). El inválido no puede contaminar una medida: `avg`, `count(col)`, `min` y
# `max` ignoran nulos, así que sale de las cuatro solo.
hechos = (
    con_precio.groupBy("id_tienda", "id_producto", "_quincena")
    .agg(
        # decimal(10,4) y no (10,2): el promedio de dos precios de dos decimales puede
        # tener tres, y redondearlo aquí metería un sesgo que no está en el dato.
        F.avg("precio_tipado").cast("decimal(10,4)").alias("precio_promedio"),
        F.count("precio_tipado").alias("observaciones"),
        # El motivo —nulo en la fuente o cadena que no castea— no se guarda aquí: se lee en
        # bronze filtrando por `(_quincena, _intento)`, que es donde vive la fila original.
        F.count_if(F.col("precio_tipado").isNull()).alias("observaciones_invalidas"),
        F.min("precio_tipado").alias("precio_min"),
        F.max("precio_tipado").alias("precio_max"),
        F.max("_intento").alias("_intento"),
    )
    .withColumn(
        # `quincena_inicio` es conversión, no derivación: es la misma quincena en un tipo
        # con el que se puede comparar y unir. Gold la necesita para resolver qué salario
        # regía, y una etiqueta de texto no se une contra un rango de vigencia.
        "quincena_inicio",
        F.to_date(
            F.concat(
                F.substring("_quincena", 1, 7),                                      # "2024-01"
                F.when(F.col("_quincena").endswith("q1"), "-01").otherwise("-16"),   # q1 el día 1, q2 el 16
            )
        ),
    )
    # Se consume tres veces —el conteo de inválidas, la escritura y el `count` del resumen—
    # y cada una rearmaría la agregación desde bronze. Antes el cache estaba río arriba,
    # sobre las filas sin agrupar, porque de ahí colgaban el hecho y la cuarentena.
    .cache()
)

# Lo inválido sale por el resumen y no por una tabla paralela: es la cifra que nb_40_dq
# compara contra umbral, y la única que hace visible que un lote llegó sucio. Con el lote
# vacío `sum` da nulo, no cero.
invalidas = hechos.agg(
    F.sum("observaciones_invalidas").alias("observaciones"),
    F.count_if(F.col("observaciones") == 0).alias("celdas_sin_precio"),
).first()
apunta(
    "invalidas",
    observaciones=int(invalidas["observaciones"] or 0),
    celdas_sin_precio=int(invalidas["celdas_sin_precio"]),
)

reemplaza_quincenas(hechos, TABLA_HECHOS, quincenas_pendientes)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
