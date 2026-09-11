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

# Qué quincenas recalcula la corrida. Vacío —el 99%— lo deriva de `pendientes_silver`.
# Con valores fuerza esas quincenas aunque `(_quincena, _intento)` no haya cambiado: un
# cambio de reglas del hecho no toca el linaje, así que la comparación de estados no puede
# verlo. `todas` es el uso normal de esa rama y evita transcribir las 46.
#
# Cadena y no lista: los base parameters de la actividad de notebook sólo llevan string,
# int, float y bool, y una lista literal se rompería al cablearla desde `pl_silver`.
quincenas_pedidas = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver de Profeco: tipa, resuelve identidad y agrega a quincena. Lee de bronze el
# intento vigente de cada quincena y recalcula lo pendiente, o lo que diga el parámetro,
# así que el backfill y la corrida del cron son el mismo código. Decisiones #12 y #13.
#
# Nada se escribe hasta que pasan todas las compuertas: lo que no cumple detiene la corrida
# (decisión #15). El tipado no lleva compuerta propia porque nb_00_config enciende ANSI.
#
# El guard de lakehouse por defecto, `ruta_tabla`, `BRONZE`/`SILVER`, `CORRIDA`,
# `DeltaTable`, el trío del resumen, las compuertas, `de_bronze`, `clave` y el `upsert`
# genérico vienen de nb_00_config. CONASAMI va aparte, en nb_21: su dimensión se mueve una
# vez al año y no cuelga de este lote.

# Silver la leen Spark y el SQL endpoint, no Power BI: `readHeavyForSpark` prende optimize
# write —que compacta antes de escribir, y hace falta desde que el hecho dejó de estar
# particionado— y deja el V-Order apagado, que sólo paga donde lee Direct Lake. Explícito
# porque con High concurrency la sesión se comparte entre los notebooks de un pipeline.
spark.conf.set("spark.fabric.resourceProfile", "readHeavyForSpark")

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

# Cuántas presentaciones puede traer el lote una vez excluidas esas dos (decisión #13).
SKUS_CANASTA = 9

LLAVE_PRODUCTO = ["presentacion", "marca"]
ATRIBUTOS_PRODUCTO = ["producto", "categoria"]

# Las columnas de tienda se normalizan antes de todo lo demás porque de su texto cuelga la
# identidad: `id_tienda` es `xxhash64(nombre_comercial, direccion)`, así que cada grafía es
# una tienda distinta. Profeco escribe la misma tienda con acento y sin él, y desde 2026
# también con el acento perdido como "?" —4,484 identidades donde hay 2,956— (decisión #39).
#
# Las de producto quedan fuera, medido: no traen "?" ni deriva de acentos, y `presentacion`
# es la llave de `NOMBRE_COMERCIAL` en gold, que la espera con acento.
COLUMNAS_TEXTO = [
    "nombre_comercial",
    "direccion",
    "cadena_comercial",
    "giro",
    "municipio",
    "estado",
]

# Los doce primeros pares pliegan el acento; los dos últimos reparan. "ð" es una "ñ" mal
# decodificada —0xF0 por 0xF1, en "Muðoz" y "Viða del Mar"— y "´" es un apóstrofo suelto
# usado como tal, en "O´farril". La "ñ" no se pliega: es letra propia y no colapsa ninguna
# identidad de más. Plegar y no acentuar porque sólo esa dirección es determinista, y porque
# `CLAVE_ESTADO` de gold espera los 30 estados sin acento, como el TopoJSON del mapa.
ACENTOS = ("áéíóúüÁÉÍÓÚÜð´", "aeiouuAEIOUUñ'")

# Palabras que Profeco publica con el acento perdido, y su forma canónica. Cada destino es
# un valor que la propia fuente escribe limpio en otra quincena: no se imputa la letra, se
# copia. El desempate de las once que tenían más de un candidato salió de la cadena gemela
# completa y no de la frecuencia global, que se habría equivocado en cuatro —"Vi?a" da
# "Viga" por frecuencia y "Viña" por contexto—.
#
# Congelado y no derivado en cada corrida: estas palabras están en la llave, y un mapa que
# se recalcula puede resolver distinto al llegar una quincena y mover `id_tienda` en
# silencio. "Maron" no lleva "?" y va aquí porque es la grafía limpia de la misma papelería,
# la única que la fuente escribe de las dos formas sin que una sea la corrupción de la otra.
#
# Riesgo residual conocido: el mapa es por palabra y generaliza, así que un valor nuevo cuya
# palabra ya esté aquí no dispara compuerta. Un "Quintana R?o" quedaría "Quintana Rio"; la
# evidencia es 5,754 a 4 a favor de "Rio", pero no es imposible.
PALABRAS = {
    "?greda": "Agreda",
    "?guila": "Aguila",
    "?lvarez": "Alvarez",
    "?lvaro": "Alvaro",
    "?ngel": "Angel",
    "?nica": "Unica",
    "?vila": "Avila",
    "Acatl?n": "Acatlan",
    "Agaler?a": "Agaleria",
    "Agust?n": "Agustin",
    "Alb?n": "Alban",
    "Alem?n": "Aleman",
    "Ampliaci?n": "Ampliacion",
    "An?huac": "Anahuac",
    "Anah?": "Anahi",
    "Anax?goras": "Anaxagoras",
    "Ant?n": "Anton",
    "Arag?n": "Aragon",
    "Atizap?n": "Atizapan",
    "Av?cola": "Avicola",
    "Aztl?n": "Aztlan",
    "B?rcenas": "Barcenas",
    "Baj?o": "Bajio",
    "Bol?var": "Bolivar",
    "C?mara": "Camara",
    "C?rdenas": "Cardenas",
    "Callej?n": "Callejon",
    "Canc?n": "Cancun",
    "Cant?n": "Canton",
    "Carnicer?a": "Carniceria",
    "Cat?lica": "Catolica",
    "Ceil?n": "Ceilan",
    "Ch?vez": "Chavez",
    "Chapay?n": "Chapayan",
    "Cob?": "Coba",
    "Col?n": "Colon",
    "Concepci?n": "Concepcion",
    "Constituc?on": "Constitucion",
    "Constituci?n": "Constitucion",
    "constituci?n": "constitucion",
    "Coraz?n": "Corazon",
    "Cort?nez": "Cortinez",
    "Costituci?n": "Costitucion",
    "Coyoac?n": "Coyoacan",
    "Cremer?a": "Cremeria",
    "Cuatitl?n": "Cuatitlan",
    "Cuautitl?n": "Cuautitlan",
    "Culiac?n": "Culiacan",
    "D?az": "Diaz",
    "D?tais": "D'tais",
    "Delegaci?n": "Delegacion",
    "Desviaci?n": "Desviacion",
    "Divisi?n": "Division",
    "Dom?nguez": "Dominguez",
    "Echeverr?a": "Echeverria",
    "Ecolog?a": "Ecologia",
    "Egu?a": "Eguia",
    "El?as": "Elias",
    "Encarnaci?n": "Encarnacion",
    "Fr?as": "Frias",
    "Fr?o": "Frio",
    "Front?n": "Fronton",
    "G?lvez": "Galvez",
    "G?mez": "Gomez",
    "Galer?as": "Galerias",
    "Galv?n": "Galvan",
    "Garc?a": "Garcia",
    "Gonz?lez": "Gonzalez",
    "H?per": "Hiper",
    "Her?ico": "Heroico",
    "Hern?n": "Hernan",
    "Hern?ndez": "Hernandez",
    "Hiliana?s": "Hiliana's",
    "Ixtlix?chitl": "Ixtlixochitl",
    "Jab?n": "Jabin",
    "Jard?n": "Jardin",
    "Jes?s": "Jesus",
    "Joaqu?n": "Joaquin",
    "Ju?rez": "Juarez",
    "Jugueter?as": "Jugueterias",
    "Jur?dico": "Juridico",
    "L?pez": "Lopez",
    "L?zaro": "Lazaro",
    "Le?n": "Leon",
    "Lecher?a": "Lecheria",
    "Liberaci?n": "Liberacion",
    "Lim?n": "Limon",
    "M?": "Mi",
    "M?jica": "Mujica",
    "M?rmol": "Marmol",
    "M?rtires": "Martires",
    "M?s": "Mas",
    "Ma?z": "Maiz",
    "Mag?n": "Magon",
    "Malec?n": "Malecon",
    "Mar?a": "Maria",
    "Mar?n": "Marin",
    "Maron": "Marin",
    "Mart?": "Marti",
    "Mart?n": "Martin",
    "Mart?nez": "Martinez",
    "Mediterr?neo": "Mediterraneo",
    "Mercer?a": "Merceria",
    "Michoac?n": "Michoacan",
    "Miner?a": "Mineria",
    "Mir?n": "Miron",
    "Misi?n": "Mision",
    "Mor?n": "Moran",
    "Mu?oz": "Muñoz",
    "Muebler?a": "Muebleria",
    "N?emero": "Nuemero",
    "N?m": "Num",
    "N?mero": "Numero",
    "Nen?far": "Nenufar",
    "Nezahualc?yothl": "Nezahualcoyothl",
    "Nezahualc?yotl": "Nezahualcoyotl",
    "Nic?las": "Nicolas",
    "Nicol?s": "Nicolas",
    "O?farril": "O'farril",
    "Obreg?n": "Obregon",
    "Ocean?a": "Oceania",
    "Ocotl?n": "Ocotlan",
    "Oreg?n": "Oregon",
    "Ort?z": "Ortiz",
    "Oxolot?n": "Oxolotan",
    "Pa?tu": "Pa'tu",
    "Pac?fico": "Pacifico",
    "Panader?a": "Panaderia",
    "Panader?as": "Panaderias",
    "Panam?": "Panama",
    "Pante?n": "Panteon",
    "Papeler?a": "Papeleria",
    "Papeler?as": "Papelerias",
    "Paricut?n": "Paricutin",
    "Pasteler?a": "Pasteleria",
    "Patr?n": "Patron",
    "Per?": "Peru",
    "Pescader?a": "Pescaderia",
    "Pescader?as": "Pescaderias",
    "Pit?goras": "Pitagoras",
    "Plut?n": "Pluton",
    "Poller?a": "Polleria",
    "Potos?": "Potosi",
    "Prolongaci?n": "Prolongacion",
    "R?o": "Rio",
    "R?os": "Rios",
    "R?stico": "Rustico",
    "Ram?n": "Ramon",
    "Ram?rez": "Ramirez",
    "Ray?n": "Rayon",
    "Reelecci?n": "Reeleccion",
    "Regi?n": "Region",
    "Renter?a": "Renteria",
    "Rep?blica": "Republica",
    "Revoluci?n": "Revolucion",
    "Rinc?n": "Rincon",
    "Robert?s": "Robert's",
    "Roc?o": "Rocio",
    "Rodr?guez": "Rodriguez",
    "Ru?z": "Ruiz",
    "S?enz": "Saenz",
    "S?nchez": "Sanchez",
    "S?per": "Super",
    "Sa?l": "Saul",
    "Sa?nz": "Sainz",
    "Secci?n": "Seccion",
    "Serd?n": "Serdan",
    "Sim?n": "Simon",
    "sim?n": "simon",
    "Su?rez": "Suarez",
    "Tec?mac": "Tecamac",
    "Tecnol?gico": "Tecnologico",
    "Teotihuac?n": "Teotihuacan",
    "Tonal?": "Tonala",
    "Torre?n": "Torreon",
    "Tortiller?a": "Tortilleria",
    "Tortiller?as": "Tortillerias",
    "Tr?fico": "Trafico",
    "Tr?nsito": "Transito",
    "Tradici?n": "Tradicion",
    "Tul?m": "Tulum",
    "Tultitl?n": "Tultitlan",
    "Uni?n": "Union",
    "V?veres": "Viveres",
    "V?zquez": "Vazquez",
    "Vel?zquez": "Velazquez",
    "Vi?a": "Viña",
    "Vild?sola": "Vildosola",
    "Villasunci?n": "Villasuncion",
    "Xoxocotl?n": "Xoxocotlan",
    "Zahuatl?n": "Zahuatlan",
}

# Los cinco typos de la fuente que parten una tienda en dos y que no son codificación:
# singular contra plural y mayúscula contra minúscula, dentro del mismo año. Elegidos a
# mano y no por frecuencia, que se equivocaría en tres de los cinco.
#
# Por columna y no en un solo mapa, y esto importa: "Central de Abasto" en singular es
# también un `giro`, y ahí es la llave de `CANAL` en gold. Renombrarlo al plural con el de
# `cadena_comercial` dejaría a gold sin canal para 2,225 filas de precio.
TYPOS = {
    "cadena_comercial": {
        "Central de Abasto": "Central de Abastos",
        "Neto (super Precio)": "Neto (Super Precio)",
        "Loyep (uniformes)": "Loyep (Uniformes)",
        "San Francisco de Asis (ropa y Telas)": "San Francisco de Asis (Ropa y Telas)",
    },
    "giro": {
        "Tienda Departamentales": "Tiendas Departamentales",
    },
}

# Los únicos no-ASCII que el texto de tienda puede traer después de normalizar, medidos en
# las 62 quincenas: ñ, el grado de "N° 3600", la exclamación invertida y los dos ordinales.
PERMITIDOS = "ñÑ°¡ºª"

# Lo que detiene la corrida: fuera del ASCII imprimible y de PERMITIDOS, o un "?" que
# sobrevivió a PALABRAS. El "?" va prohibido a propósito —en estas columnas es acento
# perdido, no signo— así que si queda uno es una palabra nueva que hay que resolver.
PROHIBIDO = rf"[^\x20-\x7E{PERMITIDOS}]|\?"

# Las columnas que identifican la fila en el mensaje de una compuerta que truena.
CONTEXTO = ("_quincena", "nombre_comercial", "direccion")

# El único import propio del notebook: lo demás llega por `%run nb_00_config`.
import re

# Una UDF y no `regexp_replace`: el mapa va de palabra a palabra y Spark no sabe sustituir
# contra un diccionario —serían 191 `regexp_replace` encadenados, que es lo que de verdad
# no escala—. La palabra se delimita con el mismo regex con que se derivó el mapa, para que
# lo que resuelve el mapa y lo que exige la compuerta sean la misma noción de palabra.
_PALABRA = re.compile(r"[A-Za-zñÑ'?]+")


@F.udf("string")
def canoniza(valor: str) -> str:
    """Cada palabra a su forma canónica, dejando la puntuación y los números donde están."""
    if valor is None:
        return None
    return _PALABRA.sub(lambda m: PALABRAS.get(m.group(0), m.group(0)), valor)


def normaliza(sdf):
    """El texto de tienda a su forma canónica, antes de cualquier compuerta y de `clave()`.

    Va aquí y no en la ingesta porque bronze no castea ni corrige: guarda lo que publicó la
    fuente, y el sha256 del manifiesto describe ese CSV (decisión #9). Resolver identidad es
    de silver.
    """
    plegado = sdf.withColumns({
        c: canoniza(F.translate(F.col(c), *ACENTOS)) for c in COLUMNAS_TEXTO
    })
    for columna, typos in TYPOS.items():
        plegado = plegado.replace(typos, subset=[columna])
    return plegado


def exige_caracteres(
    sdf, columnas: list[str], contexto: tuple[str, ...], muestra: int = 4
) -> None:
    """Truena si queda un carácter que la normalización no conoce.

    Es la compuerta que caza la deriva de codificación de la fuente sola. La "ð" de hoy la
    encontré inventariando a mano los no-ASCII de los 62 archivos; el día que Profeco emita
    otro carácter roto esto lo detiene en vez de dejarlo partir identidades en silencio.

    El mensaje lleva el carácter y su punto de código junto con la fila, porque el conteo no
    alcanza: hay que ver qué llegó y en qué tienda para decidir si va a ACENTOS o a PALABRAS.
    """
    for c in columnas:
        rotas = sdf.filter(F.col(c).rlike(PROHIBIDO))
        if rotas.take(1):
            raise RuntimeError(
                f"`{c}`: carácter que la normalización no conoce en {rotas.count():,} filas — "
                + muestra_filas(
                    rotas.withColumns({
                        "_raro": F.regexp_extract(c, PROHIBIDO, 0),
                        "_punto": F.ascii(F.regexp_extract(c, PROHIBIDO, 0)),
                    }),
                    [*contexto, c, "_raro", "_punto"],
                    muestra,
                )
            )


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


def reemplaza_quincenas(nuevas, tabla: str, quincenas: list[str]) -> None:
    """Hecho: se reescribe lo de las quincenas recalculadas y nada más.
    `replaceWhere` hace la corrida re-ejecutable sin duplicar y cuesta lo que pesan esas
    quincenas, no lo que pesa la tabla —la diferencia que importa cuando el hecho no cabe
    en memoria—. Un MERGE daría el mismo resultado leyendo la tabla entera para buscar
    filas que por construcción no existen: la quincena está completa o no está.

    El predicado va sobre `_quincena`, que no es columna de partición —la tabla es
    clusterizada—. `replaceWhere` no exige que lo sea, y valida igual que lo escrito caiga
    dentro del predicado, así que una fila de otra quincena truena en vez de colarse.
    """
    if not quincenas:
        apunta(tabla, filas=0, quincenas=0)
        return

    # "_quincena IN ('2024-01_q1', '2024-01_q2', ...)" — lo que esta corrida tiene derecho
    # a pisar.
    filtro = "_quincena IN (" + ", ".join(f"'{q}'" for q in quincenas) + ")"
    (
        nuevas.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", filtro)
        .save(ruta_tabla(tabla, SILVER))
    )
    apunta(tabla, filas=nuevas.count(), quincenas=len(quincenas))


def a_recalcular(canasta, parametro: str) -> list[str]:
    """Qué quincenas recorre esta corrida: el parámetro si lo hay, y si no el estado.

    Sustituye al drop de la tabla. `reemplaza_quincenas` con las 46 es esa misma
    reconstrucción, atómica y sin la ventana sin tabla que el drop abre —si el rerun
    truena, ahí no queda nada, lo contrario de lo que compró la decisión #15—.
    """
    pedidas = [q.strip() for q in parametro.split(",") if q.strip()]
    if not pedidas:
        return pendientes_silver(canasta)

    todas = [fila["_quincena"] for fila in canasta.select("_quincena").distinct().collect()]
    if pedidas == ["todas"]:
        return todas

    # Una quincena mal escrita dejaría el lote vacío y la corrida saldría no-op y verde,
    # que es peor que tronar: el rerun se daría por hecho sin haber recalculado nada.
    desconocidas = sorted(set(pedidas) - set(todas))
    if desconocidas:
        raise RuntimeError("quincenas que bronze no tiene — " + ", ".join(desconocidas))
    return pedidas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# `canasta` son las 46 quincenas al intento vigente sin los SKUs excluidos; `lote` es lo
# que esta corrida recalcula. Armar, validar, escribir, en ese orden y con las tres
# escrituras hasta el final.

bronze_precios = de_bronze("precios")
canasta = (
    normaliza(ultimo_intento(bronze_precios))
    .filter(~F.col("presentacion").isin(EXCLUIDOS))
    .cache()
)

quincenas_lote = sorted(a_recalcular(canasta, quincenas_pedidas))
lote = canasta.filter(F.col("_quincena").isin(quincenas_lote))

apunta("bronze_precios", filas=bronze_precios.count())
apunta("canasta", filas=canasta.count(), excluidos=len(EXCLUIDOS))
apunta(
    "lote",
    origen=quincenas_pedidas.strip() or "pendientes",
    quincenas=len(quincenas_lote),
    filas=lote.count(),
)

# Con el lote vacío —el estado estable del cron— todo lo que sigue es un no-op: el MERGE
# no encuentra nada que insertar y no commitea versión. No hace falta cortar aquí, y un
# solo punto de salida se lee mejor.

# Compuertas de entrada, y los caracteres van primero: si la normalización dejó uno que no
# conoce, todo lo que sigue estaría hasheando una grafía que no es la canónica y partiría la
# tienda en dos (decisión #39).
CONTEXTO_PRECIO = ("_quincena", "presentacion", "nombre_comercial")

exige_caracteres(lote, COLUMNAS_TEXTO, CONTEXTO_PRECIO)

# Lo vacío se ataja aquí y no con un `NOT NULL` sobre el id: las llaves no se castean, se
# hashean, y `xxhash64` salta los nulos en vez de propagarlos — una `direccion` vacía da una
# llave válida y equivocada, fusionada con otra tienda.
exige_completo(
    lote,
    LLAVE_PRODUCTO + ATRIBUTOS_PRODUCTO + ["nombre_comercial", "direccion", "precio"],
    CONTEXTO_PRECIO,
)
exige_uno_por_clave(lote, LLAVE_PRODUCTO, ATRIBUTOS_PRODUCTO)

# Los dos formatos de `presentacion` y lo que saca cada regex:
#   "Paquete con 6 Mantecadas. Vainilla (188 Gr.)"  ->  piezas=6,    gramos=188
#   "Paquete 280 Gr. Panqué Nuez"                   ->  piezas=nada, gramos=280
PIEZAS = r"Paquete con (\d+)\s"     # "Paquete con", el número, y el espacio que lo cierra
GRAMOS = r"(\d+(?:\.\d+)?) Gr\."    # el número —con decimales opcionales— antes de " Gr."
FORMATO = r"^Paquete (con \d+\s|\d+(\.\d+)? Gr\.)"   # uno de los dos y ninguno más

# Sobre el formato y no sobre `piezas`: ahí un regex roto ya no se distingue de la
# presentación que legítimamente no declara piezas.
presentaciones = lote.select("presentacion").distinct()
desconocidas = presentaciones.filter(
    ~(F.col("presentacion").rlike(FORMATO) & F.col("presentacion").rlike(GRAMOS))
).collect()
if desconocidas:
    raise RuntimeError(
        "`presentacion` con formato desconocido — "
        + ", ".join(f["presentacion"] for f in desconocidas)
    )

# Una décima presentación es un renombre —y EXCLUIDOS que dejó de excluir— o un producto
# nuevo en la canasta. Las dos se deciden.
skus = presentaciones.count()
if skus > SKUS_CANASTA:
    raise RuntimeError(f"{skus} presentaciones en el lote y la canasta son {SKUS_CANASTA}")

dim_producto = (
    lote.groupBy(*LLAVE_PRODUCTO)
    # Cualquier agregado sirve tras `exige_uno_por_clave`; `max` porque es determinista.
    .agg(*[F.max(c).alias(c) for c in ATRIBUTOS_PRODUCTO])
    .select(
        clave("presentacion", "marca").alias("id_producto"),
        "marca",
        "presentacion",
        F.coalesce(
            # El único `try_cast` del notebook: aquí el "" del regex es la presentación sin
            # piezas, no un regex roto —de eso responde la compuerta de formato—.
            F.regexp_extract("presentacion", PIEZAS, 1).try_cast("int"),
            F.lit(1),  # "Paquete 280 Gr. Panqué Nuez" no declara piezas: es uno
        ).alias("piezas"),
        # `gramos` y no `precio_por_gramo`: el precio vive en el hecho, así que la
        # división es de gold. Materializarla aquí sería guardar una columna derivable de
        # otras dos, justo lo que la decisión #12 descartó para la bandera booleana.
        F.regexp_extract("presentacion", GRAMOS, 1).cast("decimal(7,2)").alias("gramos"),
        "producto",
        "categoria",
    )
)

# Las tiendas salen del corte completo del archivo, no del de precios: si salieran de ahí
# la dimensión quedaría sesgada a las que venden pastelillos (decisión #2). Por eso este
# bloque no lee `lote` sino su propia tabla, acotada a las mismas quincenas del lote.
bronze_tiendas = de_bronze("tiendas")
tiendas_crudas = (
    ultimo_intento(bronze_tiendas)
    .filter(F.col("_quincena").isin(quincenas_lote))
)
tiendas_lote = normaliza(tiendas_crudas)

apunta("bronze_tiendas", filas=bronze_tiendas.count())

# Cuánto texto llegó con el acento perdido. Es métrica del lote y no compuerta: mide si la
# fuente se está degradando o recuperando —junio de 2026 trajo 4,378 filas y julio ninguna—
# y eso se mira, no detiene la corrida, porque PALABRAS ya lo resuelve.
apunta(
    "normaliza",
    filas_con_interrogacion=tiendas_crudas.filter(
        " OR ".join(f"`{c}` LIKE '%?%'" for c in COLUMNAS_TEXTO)
    ).count(),
)

# La clave de una tienda es `(nombre_comercial, direccion)` y es la única: búsqueda
# exhaustiva de los 255 subconjuntos de los 8 campos sobre las 46 quincenas. `direccion`
# es la del inmueble —Sears y Liverpool comparten la de la plaza— y `nombre_comercial`
# distingue al inquilino; ninguno alcanza solo (decisión #13).
LLAVE_TIENDA = ["nombre_comercial", "direccion"]
ATRIBUTOS_TIENDA = ["cadena_comercial", "giro", "estado", "municipio", "latitud", "longitud"]

# La coordenada es atributo geográfico, no identidad, y desde 2026-04_q2 la fuente da de alta
# tiendas sin geocodificar —7 Bodega Aurrera del Valle de México—. Exigirla detendría la
# cadena por tiendas que ni siquiera venden del catálogo objetivo (decisión #38).
SIN_COORDENADA = ["latitud", "longitud"]

exige_caracteres(tiendas_lote, COLUMNAS_TEXTO, CONTEXTO)

exige_completo(
    tiendas_lote,
    [c for c in LLAVE_TIENDA + ATRIBUTOS_TIENDA if c not in SIN_COORDENADA],
    CONTEXTO,
)

# Ningún atributo cambia bajo la clave en las 62 quincenas, y por eso dim_tienda no lleva
# SCD2. Sigue siendo cierto después de normalizar: los ~700 conflictos que trae 2026 son
# grafías de la fuente —acento, "?" y cinco typos— y no atributos que cambien de verdad
# (decisión #39). Que uno cambie es lo que haría falsa esa decisión: se mira, no se desempata.
exige_uno_por_clave(tiendas_lote, LLAVE_TIENDA, ATRIBUTOS_TIENDA)

dim_tienda = (
    tiendas_lote.groupBy(*LLAVE_TIENDA)
    .agg(*[F.max(c).alias(c) for c in ATRIBUTOS_TIENDA])
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
        # mueve en 46 quincenas. Es atributo geográfico para gold, y una coordenada ilegible
        # detiene la corrida como cualquier otro casteo: con ANSI el `cast` truena solo.
        F.col("latitud").cast("decimal(9,6)").alias("latitud"),
        F.col("longitud").cast("decimal(9,6)").alias("longitud"),
    )
)

# `precio` es lo único que se castea a número en el camino del hecho: `fecha_registro` viene
# en `yyyy/MM/dd` sin ambigüedad y las llaves son texto que no se convierte.
con_precio = lote.withColumns({
    "id_tienda": clave("nombre_comercial", "direccion"),
    "id_producto": clave("presentacion", "marca"),
    # `cast` y no `try_cast`: con ANSI un precio ilegible truena aquí. El vacío, que el cast
    # dejaría pasar como nulo, ya lo atajó `exige_completo`.
    "precio_tipado": F.col("precio").cast("decimal(10,2)"),
})

# El grano es tienda-SKU-quincena, no la visita: Profeco visita la misma tienda hasta
# cinco veces por quincena y `fecha_registro` no trae hora, así que las capturas del mismo
# día no se pueden ordenar. Promediar conserva las dos observaciones en vez de escoger una
# sin criterio, y `observaciones`, `precio_min` y `precio_max` dejan ver si el número se
# observó de verdad sin una bandera derivable (decisión #12).
hechos = (
    con_precio.groupBy("id_tienda", "id_producto", "_quincena")
    .agg(
        # decimal(10,4) y no (10,2): el promedio de dos precios de dos decimales puede
        # tener tres, y redondearlo aquí metería un sesgo que no está en el dato.
        F.avg("precio_tipado").cast("decimal(10,4)").alias("precio_promedio"),
        F.count("*").alias("observaciones"),
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
)

# Compuertas de salida. Un `id` repetido sólo puede ser colisión de `xxhash64`, y basta
# revisarlo en las dimensiones: el hecho apunta a esas mismas llaves.
exige_llave_unica(dim_producto, "id_producto")
exige_llave_unica(dim_tienda, "id_tienda")

# Las dimensiones antes que el hecho porque el hecho es el punto de commit:
# `pendientes_silver` lo lee para saber qué quincenas ya están hechas, así que una corrida
# que muriera entre medias deja la dimensión con filas de más —que el upsert vuelve a poner
# igual— y la quincena pendiente.
upsert(dim_producto, "dim_producto", ["id_producto"])
upsert(dim_tienda, "dim_tienda", ["id_tienda"])
reemplaza_quincenas(hechos, TABLA_HECHOS, quincenas_lote)

# El layout del hecho, declarado por el notebook que escribe: liquid clustering en lugar de
# partición. Es un ALTER idempotente y de la segunda corrida en adelante no hace nada; lo
# que lo aplica es el OPTIMIZE del mantenimiento, no la escritura.
exige_clustering(ruta_tabla(TABLA_HECHOS, SILVER), CLUSTER_HECHO)

# Un precio de cero o negativo castea perfecto y ANSI no lo ve; `gramos` es el divisor de
# `precio_por_gramo` en gold.
exige_invariantes(ruta_tabla(TABLA_HECHOS, SILVER), {"precio_positivo": "precio_min > 0"})
exige_invariantes(
    ruta_tabla("dim_producto", SILVER),
    {"gramos_positivo": "gramos > 0", "piezas_positivo": "piezas > 0"},
)

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
