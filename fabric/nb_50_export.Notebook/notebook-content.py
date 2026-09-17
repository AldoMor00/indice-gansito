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

# MARKDOWN ********************

# # Definiciones

# CELL ********************

# La copia de gold que vive fuera de Fabric: las ocho tablas como un parquet plano cada una
# en `Files/publico`, y desde prod el mismo notebook las sube en un solo commit a
# `indice-gansito-datos/publico`, de donde las lee el modelo import de la cuenta pública
# (decisión #6).
#
# Es la única salida del proyecto que va en dirección contraria. Todo lo demás lee de git y
# escribe a OneLake; esto lee de OneLake para escribir a git, y existe porque la capacidad
# pasa la mayor parte del tiempo apagada. El reporte público no puede quedarse colgado de ella.
#
# Plano y no Delta, y un archivo por tabla y no 46: el conector Web de Power BI lee un
# parquet por URL, no un directorio con su `_delta_log`. Y `overwrite`, que las capas del
# medallón nunca usan, aquí es lo correcto: esto no es una tabla con historia sino una foto
# completa de gold, y quien lleva la historia es git.

import base64

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

# Sólo prod publica. Se compara contra el nombre exacto y no contra "no es dev": un workspace
# nuevo o renombrado no escribe al repo público por omisión.
PUBLICA_DESDE = "ws-gansito-prod"
REPO = "https://api.github.com/repos/AldoMor00/indice-gansito-datos"
RAMA = "main"
# Token fine-grained con Contents: write sobre el repo de datos y nada más (`entorno.md`).
BOVEDA = "https://kv-indice-gansito.vault.azure.net/"
SECRETO = "github-indice-gansito-datos"


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

    El `coalesce(1)` es por ese nombre, no por compresión. Cuando `hechos_precios` eran 46
    particiones, fusionarlas al exportar ahorraba un tercio; desde que la tabla es un solo
    archivo el parquet exportado pesa lo que ella —378,767 contra 379,918 bytes—, así que lo
    único que sigue comprando es que Spark no reparta la salida en varias partes.
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


def github(metodo: str, recurso: str, token: str, **cuerpo) -> dict:
    """Una llamada a la API REST de GitHub sobre el repo de datos. Cualquier respuesta que no
    sea 2xx truena: no hay reintento ni fallback, la corrida se rehace completa."""
    respuesta = requests.request(
        metodo,
        f"{REPO}/{recurso}",
        json=cuerpo or None,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=60,
    )
    respuesta.raise_for_status()
    return respuesta.json()


def publica(destino: str) -> dict:
    """Los ocho parquets a `publico/` del repo de datos, en un solo commit.

    Por la API de Git Data (blobs, árbol, commit, ref) y no por la de Contents, que hace un
    commit por archivo: ocho commits por corrida dejarían el repo con fotos a medias de gold.

    Si el árbol nuevo es el mismo que el del último commit, gold no cambió y no se commitea:
    el refresh diario del reporte no necesita un commit vacío para nada.

    La ref se mueve sin `force`. Si el cron de ingesta commiteó mientras tanto, GitHub rechaza
    el avance y la corrida truena; se vuelve a correr y parte del commit nuevo.
    """
    token = notebookutils.credentials.getSecret(BOVEDA, SECRETO)

    padre = github("GET", f"git/ref/heads/{RAMA}", token)["object"]["sha"]
    arbol_padre = github("GET", f"git/commits/{padre}", token)["tree"]["sha"]

    archivos = (
        spark.read.format("binaryFile")
        .load([f"{destino}/{tabla}.parquet" for tabla in TABLAS])
        .select("path", "content")
        .collect()
    )
    entradas = [
        {
            "path": f"{DESTINO}/{archivo.path.rsplit('/', 1)[1]}",
            "mode": "100644",
            "type": "blob",
            "sha": github(
                "POST",
                "git/blobs",
                token,
                content=base64.b64encode(archivo.content).decode(),
                encoding="base64",
            )["sha"],
        }
        for archivo in archivos
    ]

    arbol = github("POST", "git/trees", token, base_tree=arbol_padre, tree=entradas)["sha"]
    if arbol == arbol_padre:
        return {"commit": "sin cambios"}

    commit = github(
        "POST",
        "git/commits",
        token,
        message=f"Publica gold de la corrida {CORRIDA}",
        tree=arbol,
        parents=[padre],
    )["sha"]
    github("PATCH", f"git/refs/heads/{RAMA}", token, sha=commit)
    return {"commit": commit[:7]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Corrida

# CELL ********************

# Se exporta siempre y completo: son ~2 MB, y una foto parcial de gold no le sirve a nadie.
# Quien decide si la corrida aportó algo es el árbol de git, no el notebook.
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

workspace = notebookutils.runtime.context["currentWorkspaceName"]
if workspace == PUBLICA_DESDE:
    apunta("github", **publica(destino))
else:
    apunta("github", commit=f"no publica desde {workspace}")

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
