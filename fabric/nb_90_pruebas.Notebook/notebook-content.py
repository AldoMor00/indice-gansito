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

# Pruebas de los helpers de nb_00_config (decisión #7). Se corre a mano y no cuelga de ningún
# pipeline: prueba código, no datos —de los datos responden las compuertas, dentro del notebook
# que escribe la tabla—.
#
# Va aparte y no dentro de nb_00_config porque `%run` copia todas las celdas del notebook
# referenciado al lugar de la llamada: viviendo allá, estas pruebas correrían en cada corrida de
# bronze y de silver, y una prueba rota tumbaría la ingesta.
#
# El criterio para que algo entre aquí es que una corrida verde no lo demuestre. Eso cubre dos
# familias. Las tres primeras son caminos de fail fast que la corrida sana no ejerce nunca, y
# vienen de que los dos bugs de bronze fueron así: un comentario que afirmaba un comportamiento
# que ninguna corrida había probado. La cuarta es lo contrario y por eso pertenece igual —un
# error en el pareo de eslabones no truena: escribe gold entero, pasa las compuertas y publica
# el número equivocado—.
#
# No escriben nada. Los DataFrames son sintéticos y de dos filas, así que correrlo no toca
# ninguna tabla ni depende de que bronze esté poblado.


def truena_con(esperado: str, corre) -> str:
    """Corre `corre` exigiendo que truene, y devuelve el primer renglón del error.

    Que no truene es el fallo, y también que truene por otra cosa: una excepción distinta a la
    esperada dejaría la prueba verde afirmando algo que no se probó. Es el único `except` del
    repo y existe porque aquí la excepción es el resultado, no el accidente.

    Spark loguea la excepción aunque se capture, así que el log sale con un ERROR rojo y un
    stack de Py4J por cada prueba que pasa. Es el ruido de que funcionó: lo que dice si algo
    falló es el exit value, y una prueba rota corta el notebook antes de llegar a él.
    """
    try:
        corre()
    except Exception as e:
        primero = str(e).splitlines()[0]
        if esperado not in str(e):
            raise AssertionError(f"tronó por otra cosa — esperaba `{esperado}`, salió: {primero}")
        return primero
    raise AssertionError(f"no tronó, y debía: {esperado}")


def prueba_cast_ansi() -> None:
    """Un precio ilegible truena en el cast en vez de dar nulo.

    Es lo que sostiene que el tipado no lleve compuerta propia (decisión #15). Con el ANSI que
    Fabric trae apagado esto daría nulo en silencio y `hechos_precios` publicaría un promedio
    sobre menos observaciones de las que declara. Ya se probó una vez extremo a extremo,
    inyectando "N/D" al lote de nb_20 —el hecho no avanzó de versión, docs/hechos.md—; aquí
    queda el núcleo, que es la parte que un cambio de configuración de la sesión puede romper.
    """
    lote = spark.createDataFrame([("18.50",), ("N/D",)], "precio string")
    # `collect` y no sólo `select`: Spark es perezoso y sin una acción el cast no se evalúa.
    error = truena_con(
        "CAST_INVALID_INPUT",
        lambda: lote.select(F.col("precio").cast("decimal(10,2)")).collect(),
    )
    apunta("cast_ansi", error=error)


def prueba_uno_por_clave() -> None:
    """`exige_uno_por_clave` truena cuando un atributo trae dos valores bajo la misma clave.

    Antes de la compuerta esto lo resolvía un `max_by` callado. Es la que sostiene que
    `dim_tienda` no lleve SCD2: la decisión se apoya en que ningún atributo cambia bajo su clave
    en 46 quincenas, así que el día que empiece a cambiar tiene que detener la corrida.
    """
    llaves, atributos = ["presentacion"], ["marca"]
    esquema = "presentacion string, marca string"

    # El caso sano primero: una compuerta que tronara siempre pasaría la prueba de abajo.
    una = spark.createDataFrame(
        [("Gansito (50 Gr.)", "Marinela"), ("Gansito (50 Gr.)", "Marinela")], esquema
    )
    exige_uno_por_clave(una, llaves, atributos)

    dos = spark.createDataFrame(
        [("Gansito (50 Gr.)", "Marinela"), ("Gansito (50 Gr.)", "Bimbo")], esquema
    )
    error = truena_con(
        "claves con un atributo cambiado",
        lambda: exige_uno_por_clave(dos, llaves, atributos),
    )
    apunta("uno_por_clave", error=error)


def prueba_clave_con_nulo() -> None:
    """`clave()` sobre una columna vacía da una llave válida y equivocada, no una nula.

    `xxhash64` salta los nulos en vez de propagarlos, así que dos claves naturales distintas
    colapsan en la misma llave y las dos filas se fusionan sin dejar rastro. Es lo que obliga a
    atajar lo vacío en la entrada, con `exige_completo`, y no con un `NOT NULL` sobre el `id`
    que nunca dispararía; por eso la prueba no termina en el peligro sino en quién lo tapa.
    """
    # Dos tiendas distintas: a una le falta la dirección, a la otra el nombre.
    tiendas = spark.createDataFrame(
        [("Oxxo", None), (None, "Oxxo")], "nombre_comercial string, direccion string"
    )
    con_llave = tiendas.select(clave("nombre_comercial", "direccion").alias("id_tienda"))
    llaves = [fila["id_tienda"] for fila in con_llave.collect()]

    if None in llaves:
        raise AssertionError(f"la llave salió nula: el nulo se propagó — {llaves}")
    if llaves[0] != llaves[1]:
        raise AssertionError(f"las dos claves naturales dieron llaves distintas — {llaves}")

    error = truena_con(
        "columnas obligatorias vacías",
        lambda: exige_completo(tiendas, ["nombre_comercial", "direccion"]),
    )
    apunta("clave_con_nulo", llave_compartida=llaves[0], atajada_por=error)


def prueba_eslabones_encadenan() -> None:
    """`eslabones` parea por período contiguo, y encadenado no es lo mismo que punta a punta.

    Es la única de las cuatro que no prueba un fail fast, y entra por la razón hermana: un
    error aquí **no truena**. Un `+ 1` que fuera `+ 2`, o el relativo al revés, dejarían las
    siete tablas de gold escritas y las compuertas verdes, y el número equivocado saldría
    publicado en el reporte. No hay corrida sana que lo delate.

    El panel rota a propósito, que es el caso que obliga a encadenar (decisión #19):

        q1   A=10  B=20
        q2   A=12  B=20  C=30
        q3         B=25  C=30

    Eslabón de q2, sobre A y B: media geométrica de 1.2 y 1.0 = raíz de 1.2.
    Eslabón de q3, sobre B y C: media geométrica de 1.25 y 1.0 = raíz de 1.25.
    Encadenado: raíz de 1.5, o sea 1.224745.

    Comparar las puntas daría 1.25 —sólo B está en q1 y en q3—, así que el número distingue
    las dos cosas. Un `+ 2` en el join uniría q1 con q3 y daría ese mismo 1.25; el relativo
    invertido daría 0.8165. Los tres errores plausibles caen en valores distintos.
    """
    precios = spark.createDataFrame(
        [
            ("A", "g", "q1", 1, 10.0), ("B", "g", "q1", 1, 20.0),
            ("A", "g", "q2", 2, 12.0), ("B", "g", "q2", 2, 20.0), ("C", "g", "q2", 2, 30.0),
            ("B", "g", "q3", 3, 25.0), ("C", "g", "q3", 3, 30.0),
        ],
        "id_tienda string, id_producto string, _quincena string, _orden int, "
        "precio_promedio double",
    )

    pares = eslabones(precios)
    if pares.count() != 4:
        raise AssertionError(f"el pareo dio {pares.count()} filas y debían ser 4")

    # El índice, tal como lo arma la medida DAX: promedio dentro del eslabón, suma entre
    # eslabones, exponencial. Si esto y el reporte se separan, el reporte miente.
    indice = (
        pares.groupBy("_quincena")
        .agg(F.avg("log_relativo").alias("media"))
        .agg(F.exp(F.sum("media")).alias("indice"))
        .first()["indice"]
    )
    esperado = 1.5 ** 0.5
    if abs(indice - esperado) > 1e-9:
        raise AssertionError(f"índice encadenado {indice} y se esperaba {esperado}")

    apunta("eslabones", pares=4, indice=round(indice, 6), puntas=1.25)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Corren en orden y la primera que falle detiene el notebook. Una prueba que falla es la misma
# noticia que una compuerta que truena, y se trata igual: se arregla y se vuelve a correr.

prueba_cast_ansi()
prueba_uno_por_clave()
prueba_clave_con_nulo()
prueba_eslabones_encadenan()

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
