# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Utilidad de dev: refresca el bronze de dev clonando el de prod (decisión #5).
# Viaja a prod porque Notebook está en el alcance del despliegue (decisión #4). Allá no
# tiene nada que hacer, así que se niega a correr.

WORKSPACE_DEV = "ws-gansito-dev"
WORKSPACE_PROD = "ws-gansito-prod"

_aqui = notebookutils.runtime.context["currentWorkspaceName"]
if _aqui != WORKSPACE_DEV:
    raise RuntimeError(
        f"""Este es un notebook de dev: clona el bronze de prod para refrescar el de dev.
        Corriéndolo en {_aqui} no tiene nada que hacer, y por eso no corre.
        Está publicado aquí sólo porque el despliegue incluye todos los Notebook.
        """
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# `sempy` es lo único que resuelve un workspace por nombre: notebookutils sabe el de la
# corrida y nada más. Viene con el runtime, así que no rompe la regla #5.
import sempy.fabric as fabric

PROD = fabric.resolve_workspace_id(WORKSPACE_PROD)

# Con la tabla vacía, `ruta_tabla` da el directorio `Tables/dbo` del lakehouse. Se
# resuelve una vez por lado y las tablas se pegan: adentro del ciclo serían dos llamadas
# a la API por tabla para armar la misma ruta.
DIR_DEV = ruta_tabla("", "lh_bronze")
DIR_PROD = ruta_tabla("", "lh_bronze", PROD)

# Se clona lo que prod tenga, no una lista que haya que mantener al día. `ls` truena si
# la ruta no existe, así que una lista vacía es prod vacío y no un error de ruta.
tablas = sorted(f.name for f in notebookutils.fs.ls(DIR_PROD))
if not tablas:
    raise RuntimeError(
        f"El bronze de {WORKSPACE_PROD} está vacío: no hay nada que clonar. "
        "Correr el pipeline de bronze en prod antes que esto."
    )
print(f"bronze de prod: {len(tablas)} tablas {tablas}")

for tabla in tablas:
    # OR REPLACE: el clon es el estado inicial de dev, no un merge. Lo que dev le haya
    # escrito a esta tabla se pierde, que es justo para lo que se corre esto.
    spark.sql(
        f"CREATE OR REPLACE TABLE delta.`{DIR_DEV}{tabla}` "
        f"SHALLOW CLONE delta.`{DIR_PROD}{tabla}`"
    )
    print(f"{tabla}: clonada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
