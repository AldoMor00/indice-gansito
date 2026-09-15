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

# Refresca el bronze de dev clonando el de prod (decisión #5). Corre al final de
# pl_mantenimiento en prod, detrás del VACUUM que rompe el clon (decisión #33), y a mano
# desde dev cuando haga falta.
#
# Los dos extremos van por nombre y no por el workspace de la corrida: así da igual desde
# dónde se lance, y el `rm` sólo puede caer en dev. En prod corre con la identidad de quien
# modificó el pipeline al último —la cuenta del despliegue—, que por eso es Contributor en
# dev (fabric/README.md).

# `sempy` es lo único que resuelve un workspace por nombre: notebookutils sabe el de la
# corrida y nada más. Viene con el runtime, así que no rompe la regla #5.
import sempy.fabric as fabric

ORIGEN = "ws-gansito-prod"
DESTINO = "ws-gansito-dev"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# El `rm` sólo puede caer en dev, y se exige contra el literal y no contra las constantes:
# invertirlas pasaría la guarda de abajo y borraría el bronze de prod.
if DESTINO != "ws-gansito-dev":
    raise RuntimeError(f"el destino del clon es {DESTINO}: el rm sólo puede caer en ws-gansito-dev")

ORIGEN_ID = fabric.resolve_workspace_id(ORIGEN)
DESTINO_ID = fabric.resolve_workspace_id(DESTINO)

# La guarda del `rm`. Con los nombres fijos no debería tronar nunca: si truena, alguien los
# editó, y clonar un bronze sobre sí mismo empieza por borrarlo.
if ORIGEN_ID == DESTINO_ID:
    raise RuntimeError(f"origen y destino son el mismo workspace ({ORIGEN_ID}): no se clona")

# Con la tabla vacía, `ruta_tabla` da el directorio `Tables/dbo` del lakehouse. Se
# resuelve una vez por lado y las tablas se pegan: adentro del ciclo serían dos llamadas
# a la API por tabla para armar la misma ruta.
DIR_ORIGEN = ruta_tabla("", BRONZE, ORIGEN_ID)
DIR_DESTINO = ruta_tabla("", BRONZE, DESTINO_ID)

# Se clona lo que prod tenga, no una lista que haya que mantener al día. `ls` truena si
# la ruta no existe, así que una lista vacía es prod vacío y no un error de ruta.
tablas = sorted(f.name for f in notebookutils.fs.ls(DIR_ORIGEN))
if not tablas:
    raise RuntimeError(
        f"El bronze de {ORIGEN} está vacío: no hay nada que clonar. "
        "Correr el pipeline de bronze en prod antes que esto."
    )
apunta("clon", origen=ORIGEN, destino=DESTINO, tablas=len(tablas))

for tabla in tablas:
    origen = f"{DIR_ORIGEN}{tabla}"
    destino = f"{DIR_DESTINO}{tabla}"

    # `CREATE OR REPLACE ... SHALLOW CLONE` por ruta no reemplaza: con el destino escrito
    # truena con DELTA_UNSUPPORTED_NON_EMPTY_CLONE. Se borra el directorio y el clon nace
    # limpio, que además es lo que se quiere —dev arranca en la v0 de prod y no arrastra
    # el historial de sus propias corridas—. Sólo desaparece lo que dev escribió: los
    # parquets son de prod y el clon nunca los tuvo.
    if notebookutils.fs.exists(destino):
        notebookutils.fs.rm(destino, True)

    spark.sql(f"CREATE TABLE delta.`{destino}` SHALLOW CLONE delta.`{origen}`")

    # La versión de prod que quedó clonada: es lo que dice qué tan al día está dev.
    apunta(tabla, version_origen=version_de(origen))

termina()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
