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

# <Qué hace este notebook, en dos o tres líneas, y a qué decisión responde.>
#
# Lo compartido viene de nb_00_config: `ruta_tabla`, `CORRIDA`, `DeltaTable`, `F` y el
# guard de lakehouse por defecto.

RESUMEN = {}


def apunta(paso: str, **datos) -> None:
    """Un solo lugar por donde sale un número: al log del notebook y al resumen que se
    devuelve. El `print` se queda en el snapshot; lo que el pipeline recibe y puede
    encadenar es el exit value."""
    RESUMEN[paso] = datos
    legible = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v}" for k, v in datos.items())
    print(f"{paso:<14}: {legible}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## <Bloque: constantes y funciones que usa primero ese bloque de la corrida.>

# CELL ********************

# <Definiciones del bloque.>

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Corrida

# MARKDOWN ********************

# ## <Bloque: Lote, Normalización, Compuertas de entrada, Dimensiones, Hechos...>

# CELL ********************

# <Lo que hace el bloque.>

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Escritura

# CELL ********************

# <Escrituras, layout y constraints.>

# `exit` corta el notebook, así que va al final y nada se pone después. El pipeline lo
# lee en @activity('<notebook>').output.result.exitValue.
notebookutils.notebook.exit(json.dumps({"corrida": CORRIDA, **RESUMEN}, ensure_ascii=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
