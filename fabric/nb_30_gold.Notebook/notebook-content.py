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
# Ocho tablas. Cuatro hechos y cuatro dimensiones; `dim_mes` concilia los dos granos de
# tiempo —el precio es quincenal y el salario mensual— como shrunken conformed dimension,
# así que ninguno de los dos hechos se desnaturaliza para caber en el otro.
#
# `hechos_ic_indice` es la excepción del patrón: no proyecta silver, resume la serie entera
# con un bootstrap que DAX no puede hacer. Por eso se calcula al final y sobre gold escrito.
#
# `ruta_tabla`, `clave`, `upsert`, `exige_llave_unica` y el trío del resumen vienen de
# nb_00_config. `upsert` recibe `GOLD` explícito: su default es silver, de donde salió.

# numpy y Window no salen de nb_00_config —ningún otro notebook los necesita— y el bootstrap
# sí: uno sortea las réplicas, el otro acumula la cadena dentro de cada una.
import numpy as np
from pyspark.sql import Window

# Gold la lee Direct Lake, que es donde el V-Order paga —40-60% en cold cache, contra ~10%
# en el SQL endpoint y nada en Spark— y donde su 15% de escritura más lenta se paga una vez.
# `readHeavyForPBI` lo prende junto con optimize write. Va aquí, en la escritura, y no en el
# mantenimiento: `OPTIMIZE ... VORDER` no V-Ordena hacia atrás lo que decide no reescribir
# (medido, en hechos.md).
spark.conf.set("spark.fabric.resourceProfile", "readHeavyForPBI")

# `hechos_precios` es el estado de gold, igual que en silver: qué quincenas ya se
# proyectaron. Si no existe —primera corrida— todo sale pendiente y el backfill es esta misma.
TABLA_HECHOS = "hechos_precios"
TABLA_RELATIVOS = "hechos_relativos"
TABLA_IC = "hechos_ic_indice"

# Réplicas y semilla del bootstrap. 2,000 es lo que midió la exploración, y la semilla fija
# hace que dos corridas sobre los mismos datos publiquen el mismo intervalo: un IC que se
# mueve solo entre refrescos no es publicable.
REPLICAS, SEMILLA = 2_000, 20240101

# El canal colapsa el `giro` que ya declara Profeco, en vez de mapear las 57 cadenas a mano:
# una cadena nueva llega con su giro puesto y un diccionario de cadenas se rompería en la
# quincena siguiente. Es la granularidad que sobrevive el umbral de pareo (decisión #19).
#
# Son los seis giros que venden la canasta —los mismos para los 9 SKUs que para el Gansito
# solo—; el universo de tiendas tiene 15 y los otros 9 no venden esto y se quedan sin canal.
# Un giro nuevo que sí traiga precio detiene la corrida, y así entró "Tortillerias": dos
# tiendas de San Luis Potosí con 8 de los 9 SKUs en 2026-05_q1. Va a `tradicional` por el
# tipo de establecimiento, igual que `Mercados`.
CANAL = {
    "Supermercado / Tienda de Autoservicio": "supermercado",
    "Tienda de Conveniencia": "conveniencia",
    "Farmacias": "conveniencia",
    "Mercados": "tradicional",
    "Central de Abasto": "tradicional",
    "Tortillerias": "tradicional",
}


# Profeco declara los nueve SKUs con el mismo `producto` —"Pastelillos y Pan Dulce
# Empaquetado"— y la misma `categoria`, así que lo único que los distingue es la cadena de
# `presentacion` (docs/fuentes.md). El nombre comercial es lo que el reporte necesita para
# filtrar y para el slicer, y no sale de un regex: dos de los nueve no siguen el patrón
# "Paquete con N <nombre> (M Gr.)" sino "Paquete 280 Gr. Panqué ...".
#
# Diccionario explícito, y aquí sí se justifica por lo contrario que en CANAL: la canasta es
# cerrada —9 SKUs, decisión #13— así que una presentación nueva es una alarma y no un caso que
# absorber. La compuerta de abajo la convierte en corrida detenida.
NOMBRE_COMERCIAL = {
    "Paquete con 1 Gansito (50 Gr.)": "Gansito",
    "Paquete con 1 Nito (62 Gr.)": "Nito",
    "Paquete con 2 Pinguinos (80 Gr.)": "Pingüinos",
    "Paquete con 2 Chocoroles (100 Gr.)": "Chocoroles",
    "Paquete con 6 Mantecadas. Vainilla (188 Gr.)": "Mantecadas",
    "Paquete con 6 Roles de Canela. con Pasas (365 Gr.)": "Roles de Canela",
    "Paquete con 8 Donitas. Espolvoreadas (140 Gr.)": "Donitas Espolvoreadas",
    "Paquete 280 Gr. Panqué Nuez": "Panqué Nuez",
    "Paquete 280 Gr. Panqué con Pasas": "Panqué con Pasas",
}


# El mapa de P2 pinta estados, y el `shapeMap` de Power BI casa la ubicación contra las claves
# del TopoJSON que trae para México. 28 de los 30 estados que declara Profeco casan por nombre
# —sin acentos, igual que el mapa: la fuente empezó a acentuarlos en 2026 y es `nb_20` el que
# los vuelve a plegar (decisión #39), así que quitar eso de silver rompe aquí—, pero los dos
# que no casan son los dos
# más grandes del padrón: ahí "Ciudad de Mexico" es `mx-dif` y "Estado de Mexico" es `mx-mex`.
# Sin traducirlos el mapa deja en blanco 634 de las 2,392 tiendas.
#
# Van los 32 y no los 30 observados: Colima y Nayarit no están en el panel —Profeco no los
# visita— y el día que aparezcan el mapa tiene que pintarlos sin volver aquí. El estado que no
# esté en el diccionario detiene la corrida, igual que la presentación sin nombre.
CLAVE_ESTADO = {
    "Aguascalientes": "mx-agu",
    "Baja California": "mx-bcn",
    "Baja California Sur": "mx-bcs",
    "Campeche": "mx-cam",
    "Chiapas": "mx-chp",
    "Chihuahua": "mx-chh",
    "Ciudad de Mexico": "mx-dif",
    "Coahuila": "mx-coa",
    "Colima": "mx-col",
    "Durango": "mx-dur",
    "Estado de Mexico": "mx-mex",
    "Guanajuato": "mx-gua",
    "Guerrero": "mx-gro",
    "Hidalgo": "mx-hid",
    "Jalisco": "mx-jal",
    "Michoacan": "mx-mic",
    "Morelos": "mx-mor",
    "Nayarit": "mx-nay",
    "Nuevo Leon": "mx-nle",
    "Oaxaca": "mx-oax",
    "Puebla": "mx-pue",
    "Queretaro": "mx-que",
    "Quintana Roo": "mx-roo",
    "San Luis Potosi": "mx-slp",
    "Sinaloa": "mx-sin",
    "Sonora": "mx-son",
    "Tabasco": "mx-tab",
    "Tamaulipas": "mx-tam",
    "Tlaxcala": "mx-tla",
    "Veracruz": "mx-ver",
    "Yucatan": "mx-yuc",
    "Zacatecas": "mx-zac",
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


def intervalo_del_indice(relativos, calendario):
    """El IC 95% del índice encadenado, quincena por quincena, por bootstrap de tiendas.

    Remuestrea **tiendas** y no celdas porque la tienda es la unidad de muestreo de Profeco.
    Las celdas de una misma tienda no son observaciones independientes —comparten dueño,
    política de precios y visita—, así que remuestrearlas sueltas daría un intervalo
    falsamente angosto.

    No se puede calcular en DAX, y por eso esta tabla existe. El error del encadenado no es
    función de los agregados del eslabón: las mismas tiendas reaparecen eslabón tras eslabón
    y sus errores se telescopan, así que sumar varianzas en cuadratura da 2.62 pp contra los
    1.60 que da el bootstrap (docs/hechos.md).

    El grano es SKU × quincena porque el índice que se publica es el de un SKU —el titular es
    el Gansito, no la canasta— y el intervalo de la canasta no se parece: promedia nueve
    series y sale mucho más angosto. Entre SKUs no se agrega, así que la medida que lo lee
    exige un solo producto en contexto, y se apaga ante cualquier filtro de tienda.
    """
    # Grano de la unidad de muestreo: lo que una tienda aporta a un eslabón de un SKU.
    # Pre-agregar aquí es lo que vuelve viable el remuestreo —el join de abajo se lleva
    # tienda × SKU × quincena y no las celdas— y no mueve el estimador: el peso multiplica
    # suma y conteo por igual, así que la media ponderada es la que promedia la medida DAX.
    por_tienda = (
        relativos.join(calendario.select("id_quincena", "orden"), "id_quincena")
        .groupBy("id_producto", "id_tienda", "orden")
        .agg(F.sum("log_relativo").alias("suma"), F.count("log_relativo").alias("celdas"))
    )

    # Cada SKU se remuestrea contra **su propio** padrón de tiendas, no contra el global: las
    # que no lo venden no son parte de su muestra, y meterlas cambiaría el n del que depende
    # el ancho del intervalo. Por eso el sorteo se hace producto por producto.
    padron = {}
    for f in por_tienda.select("id_producto", "id_tienda").distinct().collect():
        padron.setdefault(f["id_producto"], []).append(f["id_tienda"])

    rng = np.random.default_rng(SEMILLA)
    sorteos = []
    for id_producto, tiendas in sorted(padron.items()):
        # Multinomial y no un sorteo fila por fila: es el mismo bootstrap —n tiendas con
        # reemplazo en cada réplica— escrito como cuántas veces salió cada una, que es lo que
        # el join pide. Sólo viajan las que salieron: en cada réplica se queda fuera un 37%.
        conteos = rng.multinomial(
            len(tiendas), np.full(len(tiendas), 1 / len(tiendas)), size=REPLICAS
        )
        replica, columna = np.nonzero(conteos)
        sorteos.append(
            pd.DataFrame(
                {
                    "id_producto": id_producto,
                    "replica": replica,
                    "id_tienda": np.asarray(sorted(tiendas))[columna],
                    "peso": conteos[replica, columna],
                }
            )
        )

    pesos = spark.createDataFrame(
        pd.concat(sorteos, ignore_index=True),
        "id_producto bigint, replica int, id_tienda bigint, peso int",
    )

    # Media ponderada dentro del eslabón y suma acumulada entre eslabones: la agregación de la
    # medida DAX, repetida REPLICAS veces por SKU. La ventana va sobre `orden` porque el índice
    # de una quincena es el producto de todos los eslabones hasta ella.
    serie = (
        por_tienda.join(pesos, ["id_producto", "id_tienda"])
        .groupBy("id_producto", "replica", "orden")
        .agg(
            (
                F.sum(F.col("peso") * F.col("suma"))
                / F.sum(F.col("peso") * F.col("celdas"))
            ).alias("media")
        )
        .withColumn(
            "indice",
            F.exp(
                F.sum("media").over(
                    Window.partitionBy("id_producto", "replica").orderBy("orden")
                )
            )
            * 100,
        )
    )

    # `percentile` exacto y no `percentile_approx`: con 2,000 réplicas cuesta nada, y el
    # aproximado metería su propio error dentro del intervalo que se publica. Por SQL porque
    # así se escribe igual sin depender de en qué versión de PySpark salió el wrapper.
    #
    # La desviación del índice base 100 **es** la del cambio acumulado en puntos porcentuales,
    # que es la cifra con la que se compara contra lo medido en la exploración.
    return serie.groupBy("id_producto", "orden").agg(
        F.expr("percentile(indice, 0.025)").alias("ic_inferior"),
        F.expr("percentile(indice, 0.975)").alias("ic_superior"),
        F.stddev("indice").alias("ee_cambio_pp"),
    )


def reemplaza_quincenas(nuevas, tabla: str, quincenas: list[str]) -> None:
    """Hecho: se reescribe lo de las quincenas recalculadas y nada más.
    Mismo patrón que en silver —la quincena está completa o no está— pero contra gold.

    El predicado va sobre `_quincena`, que no es columna de partición: las dos tablas son
    clusterizadas. Delta valida igual que lo escrito caiga dentro del predicado, así que
    una fila de otra quincena truena en vez de colarse.
    """
    if not quincenas:
        apunta(tabla, filas=0, quincenas=0)
        return

    filtro = "_quincena IN (" + ", ".join(f"'{q}'" for q in quincenas) + ")"
    (
        nuevas.write.format("delta")
        .mode("overwrite")
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

# Al `estado` sí se le exige la dimensión completa, al revés que a `canal`: aquí no se le pide
# a la fuente que clasifique lo que no medimos —los 30 valores que escribe ya son estados— y
# el mapa colorea el universo del archivo, no la canasta.
sin_clave = {
    f["estado"] for f in tiendas_silver.select("estado").distinct().collect()
} - CLAVE_ESTADO.keys()
if sin_clave:
    raise RuntimeError(f"`estado` sin clave de mapa en CLAVE_ESTADO — {sorted(sin_clave)}")

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
).withColumn(
    # Clave del mapa y no el nombre traducido: `estado` se queda como lo escribe Profeco.
    # Misma cadena de `when` que `canal`, y por lo mismo; la compuerta de arriba garantiza
    # que ninguna acierte en nulo.
    "clave_estado",
    F.coalesce(*[F.when(F.col("estado") == e, F.lit(c)) for e, c in CLAVE_ESTADO.items()]),
)

productos_silver = de_silver("dim_producto")

# El SKU nuevo ya lo ataja silver, que truena si el lote trae más de 9 presentaciones. Lo que
# no ve es el **renombre**: con 9 presentaciones y el regex casando, pasa limpio, pero la clave
# es xxhash64 sobre la cadena, así que nace otro `id_producto` y la dimensión —acumulativa—
# queda con la vieja y la nueva. Aquí llegaría sin nombre y `coalesce` la dejaría en nulo: un
# blanco en el slicer y P1 sin encontrar su producto. De ahí que se mire el universo completo
# de la dimensión y no sólo lo que trae precio.
sin_nombre = {
    f["presentacion"] for f in productos_silver.select("presentacion").distinct().collect()
} - NOMBRE_COMERCIAL.keys()
if sin_nombre:
    raise RuntimeError(f"`presentacion` sin nombre comercial — {sorted(sin_nombre)}")

dim_producto = productos_silver.withColumn(
    # `nombre` a secas y no `nombre_comercial`: esa ya es la de `dim_tienda` —media identidad
    # de una tienda, decisión #13— y repetirla dejaría dos columnas iguales de nombre y
    # distintas de significado en el panel de campos del reporte.
    #
    # `producto` tampoco se pisa: es el genérico de Profeco y la fuente se conserva como
    # llegó; lo derivado se agrega al lado, igual que `giro` y `canal`.
    "nombre",
    F.coalesce(
        *[F.when(F.col("presentacion") == p, F.lit(n)) for p, n in NOMBRE_COMERCIAL.items()]
    ),
)

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
        # Linaje, predicado de `replaceWhere` y clave de clustering a la vez: la etiqueta
        # legible es lo que se lee en el log y lo que acota qué tiene derecho a pisar esta
        # corrida.
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

# ---------------------------------------------------------------- intervalo

# El intervalo va después de escribir, y sobre gold ya publicado en vez de sobre el lote, por
# lo mismo: es de la serie completa. Cada quincena nueva alarga la cadena y mueve el intervalo
# de todas las anteriores, así que no hay recálculo parcial que valga —el `upsert` de abajo
# actualiza las filas que de verdad cambiaron y no commitea versión si no cambió ninguna—.
ic = intervalo_del_indice(
    spark.read.format("delta").load(ruta_tabla(TABLA_RELATIVOS, GOLD)), dim_tiempo_quincena
)

# La primera quincena de la serie no tiene eslabón: su índice es 100 en toda réplica y el
# intervalo es un punto. Va en la tabla para que la banda arranque en la base y no un eslabón
# después, que es donde el `join` la dejaría fuera. El cruce se arma contra los SKUs que de
# verdad producen eslabones, no contra `dim_producto`: uno sin pareo no tiene intervalo.
hechos_ic_indice = (
    dim_tiempo_quincena.select("id_quincena", "orden")
    .crossJoin(ic.select("id_producto").distinct())
    .join(ic, ["id_producto", "orden"], "left")
    .select(
        "id_quincena",
        "id_producto",
        # `double` y no `decimal`, como `log_relativo`: son cifras de reporte, no dinero.
        F.coalesce("ic_inferior", F.lit(100.0)).alias("ic_inferior"),
        F.coalesce("ic_superior", F.lit(100.0)).alias("ic_superior"),
        F.coalesce("ee_cambio_pp", F.lit(0.0)).alias("ee_cambio_pp"),
        "orden",
    )
)

# El cierre de la serie al resumen: es lo que deja comparar la corrida contra el 1.60 pp que
# midió la exploración. Va como rango entre SKUs y no como una sola cifra, porque el notebook
# no tiene por qué saber cuál de los nueve es el titular.
cierres = [
    f["ee_cambio_pp"]
    for f in hechos_ic_indice.filter(
        F.col("orden") == hechos_ic_indice.agg(F.max("orden")).first()[0]
    ).collect()
]
apunta(
    "intervalo",
    replicas=REPLICAS,
    skus=len(cierres),
    ee_pp=f"{min(cierres):.2f} a {max(cierres):.2f}",
)

upsert(hechos_ic_indice.drop("orden"), TABLA_IC, ["id_quincena", "id_producto"], GOLD)

# El layout de los dos hechos por quincena, declarado por el notebook que escribe: liquid
# clustering en lugar de partición. `hechos_ic_indice` no entra —su grano es SKU × quincena
# y cabe en un archivo—. Son ALTER idempotentes; lo que aplica el layout es el OPTIMIZE del
# mantenimiento, no la escritura.
exige_clustering(ruta_tabla(TABLA_HECHOS, GOLD), CLUSTER_HECHO)
exige_clustering(ruta_tabla(TABLA_RELATIVOS, GOLD), CLUSTER_HECHO)

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
