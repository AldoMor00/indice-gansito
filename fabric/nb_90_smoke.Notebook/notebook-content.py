# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# nb_90_smoke -- prueba de humo del circuito dev -> git -> main -> prod.
# No escribe nada. Reporta donde esta corriendo, para comprobar que el mismo
# codigo desplegado resuelve su propio workspace por nombre (regla 1).

import notebookutils

ctx = notebookutils.runtime.context
ws_id = ctx["currentWorkspaceId"]
ws_name = ctx["currentWorkspaceName"]

ruta_bronze = (
    f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com"
    "/lh_bronze.Lakehouse/Tables/dbo"
)

print(f"workspace : {ws_name} ({ws_id})")
print(f"bronze    : {ruta_bronze}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
