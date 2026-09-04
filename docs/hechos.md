# Hechos verificados

Lo que se midió o se probó una vez, para no volver a investigarlo. Cada punto costó una
prueba y cambia cómo se escribe el código.

No es `decisiones.md`, que dice qué se eligió y por qué: aquí no hay elección, sólo lo que
resultó ser cierto. Lo de las fuentes se mide en `fuentes.md` y aquí se resume en una línea.

## Fabric y OneLake

Los dos workspaces corren **Runtime 2.0** —Spark 4.1.1, Python 3.13.11, Delta 4.2—.

- **Fabric deja ANSI apagado**, contra el default de Spark 4, que lo trae prendido. **Se puede
  prender por sesión** con `spark.conf.set("spark.sql.ansi.enabled", "true")` desde
  `nb_00_config`, y con eso `cast` truena y `try_cast` sigue dando nulo: son dos decisiones
  distintas en vez de la misma (decisión #16). Bronze corrió con ANSI sin cambiar nada.
- **Las constraints CHECK de Delta funcionan sobre tabla por ruta.**
  `ALTER TABLE delta.\`abfss://...\` ADD CONSTRAINT` engancha sin lakehouse por defecto, y las
  puestas salen en `DeltaTable.forPath(...).detail()["properties"]` como `delta.constraints.<nombre>`,
  que es lo que las hace idempotentes. Probado con las tres de `nb_20` sobre tablas ya pobladas:
  el `ADD` valida lo que ya está.
- **Las deletion vectors vienen prendidas por defecto**
  (`spark.databricks.delta.properties.defaults.enableDeletionVectors`), así que una tabla
  nueva nace en el protocolo (3,7), con `deletionVectors` y `delta.targetFileSize.adaptive`.
  Es lo que sostiene la decisión #11: un workspace en un runtime más viejo no podría leerlas.
- **El bronze de prod está en (3,7)**, las cuatro tablas parejas: 213,772 precios, 74,180
  tiendas, 685 y 42 de CONASAMI. Un solo `append` creó cada una, así que **el camino
  incremental de `pl_bronze` nunca ha corrido en prod**: lo que F2 verificó es la
  idempotencia del no-op.
- **En dev sí corrió, y da lo que dice.** Se le borraron `2025-11_q1` y `q2` al clon y
  `nb_10_profeco` las repuso solo: 2 pendientes de 46, 6,271 filas de precios y 3,262 de
  tiendas, reconciliadas contra el manifiesto, v2 en las dos tablas y los totales de vuelta
  en 213,772 y 74,180. El `DELETE` sobre el clon es escritura de dev y prod no se enteró.
- **El `SHALLOW CLONE` de Fabric es zero copy de verdad, y sirve para lo que lo queremos**:
  funciona por ruta `abfss` sin lakehouse por defecto, entre lakehouses y **entre
  workspaces**. Medido con las 213,772 filas de precios: `_delta_log` sin un solo parquet
  propio y el origen intacto después de escribirle al clon. El clon hereda el protocolo del
  origen, así que dev sigue a prod sin administrarlo aparte. Es lo que sostiene la
  decisión #5. `currentWorkspaceName` existe en el context del notebook, así que el guard
  que impide correr una utilidad de dev en prod está probado allá.
- **El clon por ruta no reemplaza: hay que borrar el destino.** `CREATE OR REPLACE TABLE` con
  `SHALLOW CLONE` truena con `DELTA_UNSUPPORTED_NON_EMPTY_CLONE` en cuanto el destino tiene
  filas —sobre tablas por ruta el `OR REPLACE` no engancha la semántica de reemplazo—, así que
  `nb_91_clona_bronze` sólo servía sobre bronze vacío y el resto de las veces había que borrar
  las tablas a mano. Se borra el directorio con `notebookutils.fs.rm` y se clona limpio, que
  además es lo que se quiere: dev arranca en la v0 de prod en vez de arrastrar el historial de
  sus propias corridas. Verificado con las cuatro tablas, dos de ellas en v2 y con parquets
  propios, que volvieron a v0. `notebookutils.fs.exists` distingue existe de no existe sin
  tronar, así que la primera corrida sobre bronze vacío no necesita caso aparte.
- **La capacidad de trial no aguanta dos sesiones de Spark a la vez.** `pl_bronze` disparó
  sus dos actividades en el mismo segundo: una consiguió sesión de Livy y la otra se fue con
  `430 TooManyRequestsForCapacity`. Por eso las actividades del pipeline van encadenadas
  aunque las fuentes sean independientes, y por eso conviene cerrar las sesiones
  interactivas de dev antes de correr prod: las dos capacidades son la misma.
- **El 430 entre dos notebooks encadenados es intermitente, no un rezago fijo.** Dos corridas
  de `pl_bronze` con el mismo intervalo y distinto resultado: en una, `nb_11_conasami` pidió
  sesión 18s después de que `nb_10_profeco` cerrara y se fue con 430 —entró en el reintento,
  69s después—; en la otra pidió a los 19s y entró a la primera. No hay umbral que esperar:
  depende de qué más esté consumiendo la capacidad. Por eso la red es el `retry` de la
  actividad y no un `Wait` fijo, que no puede cubrir algo variable.
- **El `sessionTag` no basta para compartir sesión: hace falta el switch del workspace.** El
  tag viaja en el pipeline y sólo agrupa; quien convierte las sesiones disparadas por pipeline
  en sesiones de alta concurrencia es *Spark settings → High concurrency → For pipeline running
  multiple notebooks*. Con el tag y el switch apagado, `sessionId` distintos y
  `highConcurrencyModeStatus` en `null`. Prendido, las dos actividades comparten `sessionId` y
  el estado dice `"sessionSource": "created"` en la primera y `"attached"` en la segunda. Los
  docs no dicen cuál es el estado inicial del switch; en estos workspaces venía apagado.
- **Compartir sesión no acelera nada medible, pero elimina el 430.** Tres corridas de
  `pl_bronze`: `nb_10_profeco` tardó 79s, 63s y 95s, así que la varianza se come cualquier
  ahorro; `nb_11_conasami` quedó en 32s con y sin HC. Lo que cambia es que la segunda
  actividad deja de pedir sesión —se engancha— y el 430 entre las dos pasa de intermitente a
  imposible.
- **Un workspace se resuelve por nombre con `sempy`, no con notebookutils**:
  `fabric.resolve_workspace_id(nombre)` da el GUID —verificado contra `currentWorkspaceId`—
  y truena con `WorkspaceNotFoundException` si el nombre no existe, así que no devuelve
  basura en silencio. Hace falta porque `notebookutils.lakehouse.get` sí acepta un segundo
  workspace, pero por GUID.
- **`notebookutils.fs.ls` distingue vacío de inexistente**: sobre `Tables/dbo` da una
  entrada `isDir` por tabla, con el `name` sin diagonal; sobre una ruta que no existe
  truena con 404. Por eso `nb_91_clona_bronze` enumera el bronze de prod en vez de llevar
  una lista, y un cero es "prod está vacío", no "me equivoqué de ruta".
- **El *resource profile* del workspace decide V-Order y Optimize Write, y los workspaces
  corren `writeHeavy`.** Ese perfil trae V-Order apagado —`spark.sql.parquet.vorder.default`
  en `false`— y fija los parámetros de Optimize Write —`binSize` 128, `partitioned.enabled`—
  pero **no** su interruptor: `optimizeWrite.enabled` no queda puesto en la sesión, y son los
  perfiles `readHeavy*` los que sí lo prenden explícitamente. O sea que prender V-Order sobre
  gold, que es lo que lee Direct Lake, es cambiar de perfil o ponerlo a mano; no es algo que
  ya esté pasando.
- **Lo que rompe un clon es `OPTIMIZE` seguido de `VACUUM`** —el primero deja huérfanos los
  archivos que el clon referencia y el segundo los borra—, no `VACUUM` solo.
- **Fabric escribe `.platform` sin salto de línea final.** Con uno de más, el item sale
  `uncommitted` al sincronizar aunque el contenido sea idéntico. El `notebook-content.py`
  escrito a mano **sí** sobrevive el round-trip, celda `%run` incluida.
- **OneLake no acepta mezclar GUID y nombre** en `abfss://`: 400
  `FriendlyNameSupportDisabled`. Los dos van por GUID; ver `ruta_tabla` en `nb_00_config`.
- **Spark no habla HTTPS**: los bytes bajan con `requests` al driver. La capacidad **sí**
  sale a `raw.githubusercontent.com`, probado con texto y con binario.
- **`activityId` correlaciona el exit value con el snapshot** y `DESCRIBE HISTORY` ya es la
  bitácora de escrituras. Una tabla de runs propia las duplicaría.
- **Pero `activityId` es de la sesión, no de la ejecución**, interactivamente y por pipeline.
  Dos corridas seguidas de `nb_21` en la misma sesión interactiva salieron con el mismo
  `corrida` en el exit value, y las dos actividades de `pl_silver` también, con el `sessionId`
  que compartieron. La alta concurrencia lo garantiza en vez de evitarlo: cada actividad se
  engancha a la sesión de la primera. Así que no sirve de llave para la tabla de corridas de
  F5 —dos ejecuciones se pisan siempre, no a veces—. El id por ejecución existe del lado del
  pipeline: el `runId` de cada actividad salió distinto.

## Las fuentes

- **El estado estable del cron es no hacer nada**, y se sabe por qué: el programa de Profeco
  dejó de publicar, no cambió de ruta. Al 2026-09-01 son 20 quincenas pendientes de
  calendario, de `2025-12_q1` en adelante, y las 20 responden "no publicada": nueve meses de
  silencio sin un solo error. Medido en [`fuentes.md`](fuentes.md).
- **El esquema de la zona raw es estable**: 15 columnas en los 46 parquets de precios, 8 en
  los 46 de tiendas, todas `String`. Por eso bronze escribe con `mergeSchema` apagado.
- **La clave de una tienda es `(nombre_comercial, direccion)`**, la única mínima de los 255
  subconjuntos probados: lat/long no identifica —el 10.1% de las filas comparte coordenada—
  pero es constante, y bajo la clave no cambia ningún atributo en 46 quincenas, así que
  `dim_tienda` no tiene hoy qué versionar. Medido en [`fuentes.md`](fuentes.md).
- **Ninguna columna que silver necesita viene vacía, en ninguna de las 46 quincenas.**
  Verificado por las compuertas de `nb_20` sobre las 213,041 filas de la canasta y las 74,180
  de tiendas: `presentacion`, `marca`, `producto`, `categoria`, `nombre_comercial`,
  `direccion`, `precio`, `cadena_comercial`, `giro`, `estado`, `municipio`, `latitud` y
  `longitud`, ni nulas ni cadena vacía. Es lo que deja que las llaves se hasheen sin riesgo:
  `xxhash64` salta los nulos, así que una clave vacía daría una llave válida y equivocada en
  vez de una nula.
- **Las 213,772 filas castean.** `precio` a `decimal(10,2)`, la coordenada a `decimal(9,6)`,
  el gramaje a `decimal(7,2)`, con ANSI prendido y sin un solo fallo. Lo que sostiene la
  decisión #15: el dato sucio nunca ha existido en esta fuente.
- **Y cuando se inyecta, truena.** Un `precio = "N/D"` metido al lote sale como
  `CAST_INVALID_INPUT` en la agregación del hecho, no como nulo. `exige_completo` lo deja
  pasar —no viene vacío— y lo ataja el `cast`, que es justo el reparto de la decisión #16, y
  `hechos_precios` no avanzó de versión. Es el único camino de fail fast que la fuente no
  ejerce sola, y hasta esta prueba sólo lo respaldaba un comentario.
- **La identidad y el parseo resisten una prueba más estricta que la que los eligió.** Ningún
  atributo trae dos valores bajo su clave natural —ni en productos ni en tiendas—, las 9
  presentaciones matchean los dos formatos conocidos, y `xxhash64` no colisiona en 2,392
  tiendas ni en 9 SKUs. Antes esto lo tapaba un `max_by`; ahora truena.
- **El tabulador de CONASAMI no deja huecos: cada zona que sale del archivo es una fusión y
  ninguna vuelve.** Las 42 filas son 21 `inicio_vigencia` distintos y 6 zonas de 7 literales,
  y las tres salidas son `c` en `2012-11-27`, `a` y `b` en `2015-10-01` —entra `unica`— y
  `unica` en `2019-01-01`, cuando se crea la ZLFN. Por eso el cierre de vigencias puede ser
  global: verificado por la compuerta de `nb_21`, que truena si una zona reaparece después de
  haberse cerrado. Las 727 filas de las dos tablas castean con ANSI prendido.
- **No hay PII** en lo que se persiste, y bronze cabe de sobra en la capacidad.

## CI y despliegue

- **`parameter.yml` se queda vacío**: con el clon de la decisión #5, dev y prod comparten
  rutas y no hay nada que sustituir al desplegar. Las referencias entre items tampoco:
  medido con `pl_bronze`, al commitear desde la UI la git integration reescribe el
  `workspaceId` de la actividad al GUID nulo —"el workspace donde corro"— y el `notebookId`
  al `logicalId` del notebook, y `fabric-cicd` los resuelve al publicar porque los notebooks
  van en el mismo despliegue y antes que el pipeline. Verificado en el pipeline ya desplegado:
  trae el GUID del notebook **de prod** y el workspace de prod. El JSON que muestra la UI
  **no** es el que se despliega: ahí los dos campos son GUIDs literales del workspace vivo.
- **La git integration de Fabric corta el mensaje del commit.** Cabe el asunto y poco más:
  un mensaje de 302 caracteres se truncó a media palabra, sin avisar. Los commits que salen
  del UI se escriben de una línea y el porqué se deja en los comentarios del notebook o en
  estos documentos, que es donde de todos modos se busca.
- **`fab` se instala con `uv tool install --python 3.12`**; con 3.14 truena con pyyaml.
- **Los workflows sólo se registran desde la rama por defecto**: `workflow_dispatch` y
  `schedule` no existen mientras el archivo viva sólo en `dev`.
- **El manifiesto es el estado del pipeline, y `main` es lo que corre.** Un pipeline
  idempotente respecto de su manifiesto deja de serlo cuando el manifiesto cambia de
  dirección: `datos#2` movió la zona raw bajo `profeco/` mientras el script de `main` seguía
  leyendo el de la raíz, así que el cron lo encontró vacío y rehizo cinco quincenas en el
  layout viejo antes de que nadie lo viera ([datos#3](https://github.com/AldoMor00/indice-gansito-datos/pull/3)).
  Mover el estado de una fuente y su productor va junto, y en el mismo merge a `main`.
