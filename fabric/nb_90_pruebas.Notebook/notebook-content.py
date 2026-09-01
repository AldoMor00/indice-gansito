# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# nb_91_clona_bronze — utilidad de dev: refresca el bronze de dev clonando el de prod.
# Viaja a prod porque Notebook está en el alcance del despliegue (decisión #4). Allá no
# tiene nada que hacer, así que se niega a correr.

WORKSPACE_DEV = "ws-gansito-dev"

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
