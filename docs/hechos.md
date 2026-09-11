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
- **`replaceWhere` reescribe sólo lo suyo, y con las 46 es el full refresh.** Las cuatro ramas
  del parámetro de `nb_20` sobre `hechos_precios`: vacío no commitea versión; una quincena
  inventada truena antes de escribir; `todas` deja **una sola** versión, de un archivo y 126,493
  filas. Es lo que vuelve innecesario el drop de la tabla, que además abriría una ventana sin
  tabla.
- **Recalcular dos quincenas no reescribe el archivo grande: lo marca con una deletion vector.**
  Sobre la tabla clusterizada, un `replaceWhere` de `2025-11_q1` y `q2` dejó el archivo de
  830,440 bytes intacto con una DV de **833 bytes** sobre 4,258 filas, y escribió las nuevas en
  un archivo aparte de **33,480 bytes**. O sea que soltar la partición **no** trajo el write
  amplification que se le temía —reescribir a las quincenas vecinas por compartir archivo—: se
  escriben 33 KB donde antes se reescribían 830 KB.
- **Y esa reescritura sigue siendo determinista, ahora medido con checksum.** Dos corridas
  idénticas de esas dos quincenas dan dos archivos de 33,480 bytes con el **mismo `md5` y sin
  una diferencia en `cmp`**, y la tabla queda en 863,920 bytes las dos veces. Reconstruir no
  mueve el dato.
- **Lo que sí cuesta es la DV acumulada**: la tabla pasa de 830,440 a 863,920 bytes y ahí se
  queda hasta que alguien la purgue. Las 4,258 filas marcadas son el **3.4%** de la tabla, justo
  debajo del 5% con el que `OPTIMIZE` purga solo. Es el trabajo concreto del mantenimiento.
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
- **El `CLUSTER BY` engancha sobre tabla por ruta**, igual que el `ADD CONSTRAINT`, y el
  clustering sobrevive las escrituras posteriores. Es lo que permite el layout de los hechos
  sin lakehouse por defecto.
- **`DataFrameWriter.clusterBy` es un no-op silencioso sobre tabla por ruta.** El método
  existe en Spark 4 y no truena, pero `DESCRIBE DETAIL` devuelve `clusteringColumns` vacío:
  la tabla queda sin clusterizar. Por eso el layout se declara con un `ALTER` idempotente
  —`exige_clustering`— y no en la escritura.
- **`replaceWhere` no exige que la columna del predicado sea de partición, y el guard sigue
  vivo.** Reemplazar una quincena dejó la otra intacta —51.5 contra 55.0 en la sonda— y una
  fila fuera del predicado tronó con `DELTA_REPLACE_WHERE_MISMATCH`. Es lo que deja soltar el
  `partitionBy` sin tocar el fail fast de silver.
- **`OPTIMIZE … VORDER` no V-Ordena hacia atrás lo que decide no reescribir.** Sobre cuatro
  archivos de 10 KB escritos sin V-Order: 4 considerados, 4 saltados y `NonVOrderedFiles` con
  cero archivos, con clustering y sin él. Por eso el V-Order entra por el perfil de la
  escritura en `nb_30` y el `VORDER` del mantenimiento sólo sirve para conservarlo.
- **El perfil de recursos puesto en runtime sí aplica lo que la doc dice que trae.**
  `spark.conf.set("spark.fabric.resourceProfile", "readHeavyForPBI")` en la sesión mueve
  `spark.sql.parquet.vorder.default` de `false` a `true` y deja `optimizeWrite.enabled` en
  `true` con `binSize` de `1g`. Lo verificado es la sesión, no los bytes del parquet: el
  V-Order no se ve en el metadato de la tabla.
- **Cuánto vale cada cosa, sobre las mismas 126,493 filas de `hechos_precios` y con un archivo
  de control:**

  | escritura | archivos | bytes |
  |---|---|---|
  | sin V-Order | 1 | 830,440 |
  | con V-Order | 1 | **380,227** |
  | fuente ya V-Ordenada, reescrita con V-Order apagado | 1 | 379,483 |
  | esa misma, particionada por `_quincena` | 46 | 833,921 |

  **El V-Order vale −54.2% y particionar cuesta +120%**, y las dos cifras se parecen porque son
  la misma mecánica: qué tan bien se agrupan las filas parecidas dentro de un archivo. El
  V-Order las agrupa; partir la tabla en 46 deshace esa agrupación y obliga a 46 diccionarios y
  46 footers. El tercer renglón es el que separa las dos cosas: **el orden que impone el V-Order
  sobrevive una reescritura sin V-Order**, así que lo que se pierde al reescribir sin él son
  2.8 puntos de encoding, no el ordenamiento. Medir esto leyendo de gold —que ya está
  V-Ordenado— da ese 2.8% y esconde el resto.
- **`OPTIMIZE` no commitea versión cuando no reescribe nada, y `VACUUM` commitea dos siempre.**
  En la primera corrida de `nb_60` sobre silver `hechos_precios`, el `OPTIMIZE` no dejó versión
  —se salta la tabla por tamaño— y el `VACUUM` dejó `VACUUM START` y `VACUUM END`: Fabric trae
  prendido el logging del vacuum. O sea que **cada mantenimiento deja dos versiones nuevas en
  cada tabla aunque no se haya movido un dato**, y eso desframea al modelo Direct Lake igual.
  Es otra razón para que el refresh vaya dentro del pipeline (decisión #33).
- **El `DRY RUN` del `VACUUM` lista el `metadata/` de Iceberg en todas las tablas, y el `VACUUM`
  no lo borra.** Fabric mantiene por tabla un directorio `metadata` con la virtualización
  Iceberg de OneLake —`v1.metadata.json`, los `.avro` del snapshot, `version-hint.text`—, y el
  listado del vacuum lo toma por candidato: `numFilesToDelete: 1` contra `numDeletedFiles: 0`
  donde era el único, y `numDeletedFiles: 2` en `dim_salario_minimo`, que sí tenía dos parquets
  huérfanos. Se regenera después de cada corrida, pero por los commits nuevos, no porque lo
  hayan borrado. `nb_60` lo descuenta de su conteo para no reportar un borrado que no ocurre.
- **`VACUUM … LITE` combina con `DRY RUN`, y aquí no sirve.** Ignora el `metadata/`, que es lo
  que se le pedía, pero sobre `dim_salario_minimo` también ignoró los dos huérfanos de verdad
  que el modo completo sí borró: da cero. Arma la lista leyendo el log en vez del directorio, y
  lo que el log no alcanza no lo ve.
- **`clusteringQuality` viene dentro de las métricas de `OPTIMIZE`, en PySpark.** La doc lo
  presenta como método exclusivo de Scala: sale una fila por columna de clustering, con
  `avgDepth`, `overlapRatio` y `skippingEffectiveness`, sin llamada aparte.
- **Ninguna API pública devuelve el exit value de un notebook**: ni job instances ni
  livySessions lo traen, se queda en el snapshot. Para leerlo desde la CLI el notebook escribe
  el mismo JSON a `Files/` y se baja con `fab cp`.
- **Fabric escribe `.platform` sin salto de línea final.** Con uno de más, el item sale
  `uncommitted` al sincronizar aunque el contenido sea idéntico. El `notebook-content.py`
  escrito a mano **sí** sobrevive el round-trip, celda `%run` incluida.
- **Los base parameters de la actividad de notebook sólo llevan escalares** —`int`, `float`,
  `bool`, `string`—, y `list` y `dict` no están soportados: la doc recomienda serializar a JSON
  y deserializar dentro del notebook
  ([doc](https://learn.microsoft.com/fabric/data-engineering/author-execute-notebook#integrate-a-notebook)).
  Es lo que sostiene que `quincenas_pedidas` de `nb_20` sea una cadena y no una lista.
- **OneLake no acepta mezclar GUID y nombre** en `abfss://`: 400
  `FriendlyNameSupportDisabled`. Los dos van por GUID; ver `ruta_tabla` en `nb_00_config`.
- **Spark no habla HTTPS**: los bytes bajan con `requests` al driver. La capacidad **sí**
  sale a `raw.githubusercontent.com`, probado con texto y con binario.
- **`activityId` correlaciona el exit value con el snapshot** y `DESCRIBE HISTORY` ya es la
  bitácora de escrituras. Una tabla de runs propia las duplicaría (decisión #31).
- **Pero `activityId` es de la sesión, no de la ejecución**, interactivamente y por pipeline.
  Dos corridas seguidas de `nb_21` en la misma sesión interactiva salieron con el mismo
  `corrida` en el exit value, y las dos actividades de `pl_silver` también, con el `sessionId`
  que compartieron. La alta concurrencia lo garantiza en vez de evitarlo: cada actividad se
  engancha a la sesión de la primera. Así que no sirve de llave para una tabla de corridas
  —dos ejecuciones se pisan siempre, no a veces—. El id por ejecución existe del lado del
  pipeline: el `runId` de cada actividad salió distinto.
- **El monitoring hub retiene 30 días de actividades y 60 el historial de notebook.** La página
  de *Activities* lista hasta 100 actividades de los últimos 30 días —hasta 100 por item— y
  *Historical runs* da los 30 días completos de una sola actividad; el job history del notebook,
  snapshot incluido, vive 60 días
  ([hub](https://learn.microsoft.com/fabric/admin/monitoring-hub),
  [límites](https://learn.microsoft.com/fabric/data-engineering/notebook-limitation#other-specific-limitations)).
  Es lo que sostiene la decisión #31.
- **Los avisos de fallo viven en *Schedule failures*, y son de items programados.** Es una
  página del hub que gestiona las notificaciones de fallo de lo que corre por schedule, así que
  no hay nada que prender hasta que `pl_bronze` tenga uno.
- **La variable library no tiene tipo arreglo y su value set activo no viaja en el item.** Los
  tipos son String, Integer, Number, Boolean, DateTime, Guid y las dos referencias —item y
  connection—; cuál value set está activo lo guarda el workspace aparte de la definición, así
  que el despliegue no lo lleva
  ([doc](https://learn.microsoft.com/fabric/cicd/variable-library/variable-library-overview),
  [value sets](https://learn.microsoft.com/fabric/cicd/variable-library/value-sets)).
- **Una user data function es Python 3.11.9 serverless sin Spark**, invocable por REST desde
  notebook, pipeline, Activator y translytical. Publicar tiene cooldown de dos minutos y sólo
  el dueño del item edita el código
  ([doc](https://learn.microsoft.com/fabric/data-engineering/user-data-functions/user-data-functions-overview),
  [límites](https://learn.microsoft.com/fabric/data-engineering/user-data-functions/user-data-functions-service-limits)).
  Es lo que sostiene la decisión #18.

## La CLI de Fabric

`fab` 1.7.0, autenticada contra `app.fabric.microsoft.com`. Probado creando, actualizando y
borrando un `nb_99_prueba_cli` desechable en dev, que quedó como estaba.

- **`--format .py` no es opcional, y va en los dos sentidos.** Sin él, `fab export` baja un
  `notebook-content.ipynb` y `fab import` espera JSON: subir la plantilla de la skill falla con
  `InvalidNotebookContent`. Con él, el archivo es el mismo `notebook-content.py` de git.
- **El índice de git guarda LF y el árbol de trabajo se ve en CRLF**, y quien convierte es
  `core.autocrlf`, que en Windows viene en `true` desde `C:/Program Files/Git/etc/gitconfig`.
  Se consulta con `--show-origin`: mirar sólo `--local` y `--global` lo da por no definido y
  lleva a concluir, al revés, que el repo es CRLF y que git no convierte nada. Lo que dice la
  verdad es `git ls-files --eol`, que reporta índice y árbol por separado.
- **El fallo real es el blob con finales mezclados.** Editar línea a línea un archivo que en
  el árbol está en CRLF mete líneas con LF solo, y esa mezcla llega al índice: tres notebooks
  quedaron en `i/mixed` con 21, 43 y 43 líneas sueltas. Un blob mezclado envenena todos los
  diffs siguientes —el commit de gold marcó 330 líneas en `nb_00_config` y sólo 40 eran
  contenido, medido con `git diff --ignore-cr-at-eol`—. Se arregla con `.gitattributes` en
  `* text=auto` y `git add --renormalize .`, que no toca una sola línea de contenido.
- **`fab export` no es fiel byte por byte, y lo parece.** Cuando lo que Fabric tiene guardado
  está en CRLF, lo que baja trae los CR **duplicados** —`CR CR LF`—, así que un `diff` contra
  el archivo local marca el 100% de las líneas como cambiadas. Es artefacto de la bajada:
  con esos mismos items el panel de código fuente de la UI mostró sólo las líneas que de
  verdad se tocaron. Medido sobre `nb_00_config` (296 CR de más) y `nb_21_conasami` (192); el
  que había subido con LF baja limpio. Para verificar qué cambió, la UI y no el export.
- **`fab import` crea y actualiza con el mismo comando.** El segundo import sobre el mismo item
  reemplaza la definición; `-f` sólo se salta la confirmación.
- **El `.platform` que baja la CLI no es el de git**: trae `logicalId` en ceros y otra sangría.
  Es la definición de la API, no la representación de git —la git integration asigna el
  `logicalId`—, así que ese archivo no se copia al repo. El `notebook-content.py` sí.
- **Y el `logicalId` que se sube tampoco importa.** Al importar `nb_90_pruebas` con un GUID
  inventado en su `.platform`, el item nació en dev y al commitear desde la UI la git
  integration escribió otro. O sea que crear un notebook por CLI no obliga a acertarle: hay
  que poner uno bien formado, no uno en particular.
- El directorio de salida de `fab export` tiene que existir; si no, `InvalidPath`.
- **Un import contra un notebook abierto en el UI no se pierde ni pisa en silencio.** La
  pestaña abierta no se entera sola, pero al guardar avisa —"Another user has saved changes to
  this notebook", con *View changes* y la elección de versión—, así que el conflicto se resuelve
  a la vista. Aun así conviene refrescar antes de editar: elegir versión es todo o nada, no un
  merge por celda.

- **El mensaje de commit del UI de Fabric se trunca cerca de los 300 caracteres.** Cuerpo
  incluido: un título de 46 más dos párrafos se cortó a media palabra. Los commits que salgan de
  ahí caben en título más dos líneas, y el porqué largo va al comentario del código.
- **`fab api` antepone `https://api.powerbi.com/v1.0/myorg/`** con `-A powerbi`, así que el
  endpoint se escribe `datasets/<id>/executeQueries` y no con el prefijo completo. El cuerpo se
  lee con la codificación del sistema —cp1252 en Windows—, así que el JSON de la consulta va en
  ASCII con escapes unicode o truena con `charmap codec can't decode`.
- **La exploración no necesita notebook.** `fab api -A powerbi -X post
  datasets/<id>/executeQueries` corre DAX contra el modelo y devuelve el resultado envuelto en
  `text`, y `fab job run` corre un notebook y espera a que termine. Todo el corte transversal de
  P2 se midió así, sin crear ni borrar un item.

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
  ejerce sola, y hasta esta prueba sólo lo respaldaba un comentario. El núcleo quedó en
  `nb_90_pruebas`; lo que no se repite ahí es la parte extremo a extremo.
- **La identidad y el parseo resisten una prueba más estricta que la que los eligió.** Ningún
  atributo trae dos valores bajo su clave natural —ni en productos ni en tiendas—, las 9
  presentaciones matchean los dos formatos conocidos, y `xxhash64` no colisiona en 2,392
  tiendas ni en 9 SKUs. Antes esto lo tapaba un `max_by`; ahora truena.
- **La llave válida y equivocada tiene número.** `clave("nombre_comercial", "direccion")` da
  `8554209004007291361` tanto para `("Oxxo", null)` como para `(null, "Oxxo")`: dos claves
  naturales distintas, una sola llave, y ninguna nula. Es lo que obliga a atajar lo vacío en
  la entrada con `exige_completo` y no con un `NOT NULL` sobre el `id`, que no dispararía
  nunca. Probado en `nb_90_pruebas` junto con los otros dos caminos de fail fast, los que la
  corrida sana no ejerce. Ese notebook saca un ERROR rojo con stack de Py4J por cada prueba
  que **pasa** —Spark loguea la excepción aunque se capture—, así que lo que dice si algo
  falló es el exit value.
- **El tabulador de CONASAMI no deja huecos: cada zona que sale del archivo es una fusión y
  ninguna vuelve.** Las 42 filas son 21 `inicio_vigencia` distintos y 6 zonas de 7 literales,
  y las tres salidas son `c` en `2012-11-27`, `a` y `b` en `2015-10-01` —entra `unica`— y
  `unica` en `2019-01-01`, cuando se crea la ZLFN. Por eso el cierre de vigencias puede ser
  global: verificado por la compuerta de `nb_21`, que truena si una zona reaparece después de
  haberse cerrado. Las 727 filas de las dos tablas castean con ANSI prendido.
- **No hay PII** en lo que se persiste, y bronze cabe de sobra en la capacidad.

## El índice

Medido sobre la zona raw con el mismo grano y las mismas llaves que `nb_20` —tienda por
`(nombre_comercial, direccion)`, hecho promediado a tienda-SKU-quincena, la canasta de 9 SKUs—,
así que `nb_30` reproduce estas cifras. El SKU es `Paquete con 1 Gansito (50 Gr.)`: 14,858
celdas en 508 tiendas.

- **El panel rota mucho más de lo que sugiere el agregado.** El 24.3% de `fuentes.md` es del
  universo de tiendas; para la serie del Gansito sólo **31 de 508 tiendas (6.1%)** aparecen en
  las 46 quincenas. Pero eslabón a eslabón el traslape es alto: la mediana es de **286 tiendas
  pareadas** contra 332 observadas por quincena, o sea que el encadenamiento **retiene el 86%**
  del n disponible. La rotación asusta en el agregado y es inofensiva entre quincenas vecinas.
- **La cobertura del panel cae con el detalle del corte, y a nivel estado no cae.** Mediana de
  los 45 eslabones: el pareo conserva el **86.3%** de las tiendas observadas, el **95.1%** de las
  cadenas, el **95.2%** de los municipios y el **100%** de los estados. El encadenado suelta
  tiendas y no geografía, que es lo que deja leer P2 y P3 sobre la misma muestra. El eslabón más
  flaco del Gansito trae **177 tiendas pareadas** contra un umbral de 30, así que ninguno de los
  45 queda bajo la guarda (decisión #30).
- **El sesgo de composición existe y es chico: 1.2 puntos.** Promedio simple de punta a punta
  da **+25.63%**; el encadenado sobre tiendas pareadas, **+24.40%**. El precio del Gansito subió
  de verdad, y el promedio simple no mentía mucho —pero mentía en dirección desconocida—.
- **Las cuatro variantes, con bootstrap de 2,000 réplicas sobre tiendas:**

  | variante | cambio | EE | IC 95% | n que usa |
  |---|---|---|---|---|
  | promedio simple | +25.63% | 1.11 | 23.48 – 27.76 | 100% |
  | panel balanceado | +31.75% | 3.09 | 25.91 – 38.03 | 9% |
  | pareo en las puntas | +29.55% | — | — | 46% |
  | Jevons encadenado | +24.40% | 1.60 | 21.36 – 27.64 | 86% |

  El panel balanceado es lo peor de los dos mundos: el más sesgado **y** el más impreciso. Su
  cifra alta es sesgo de supervivencia —las tiendas que duran dos años son las formales—.
- **Parear cuesta precisión, pero poco, y por dos razones medibles.** El EE sube de 1.11 a 1.60
  pese a encadenar 45 eslabones, porque (a) el pareo cancela la dispersión que no importa —la
  desviación de log-precios *entre* tiendas es 0.1344 y la del *cambio dentro* de una tienda es
  0.0647, **2.1x menos**— y (b) las mismas tiendas reaparecen eslabón tras eslabón, así que sus
  errores se telescopan: sumar varianzas a lo bruto da 2.62 pp y el bootstrap da 1.60.
- **Carli sobreestima 12 puntos.** Media aritmética de relativos: **+37.06%**, contra +24.40%
  de Jevons y **+23.90%** de Dutot. Dos tiendas que intercambian precios —+100% y −50%—
  promedian +25% en Carli cuando en conjunto no pasó nada. Dutot y Jevons quedan a medio punto
  entre sí porque el agregado elemental es un solo SKU homogéneo.
- **El umbral de pareo decide la granularidad del corte, no el gusto.** Con el `giro` crudo de
  Profeco sólo **una de cinco** categorías junta 30 tiendas pareadas en su peor eslabón
  (Supermercado, 135). Colapsado a tres canales pasan dos: supermercado (135, **+29.84%**) y
  conveniencia (31, **+11.35%**); tradicional se queda en 7.
- **El supermercado alcanzó a conveniencia a mediados de 2024, y desde entonces no hay brecha
  estable.** Arrancó $3.49 más barato, se encareció de $15.89 a casi $20 entre enero y junio de
  2024 —conveniencia se quedó en ~$20— y para agosto la diferencia era $0.17. En las 46
  quincenas la brecha va de $4.09 a −$0.89, media $0.69: **es negativa en 19**, o sea que el
  supermercado es el caro buena parte de 2025.
- **El deflactor sale de CONASAMI y da +6.81%** en la ventana: INPC 133.554 en `2024-01` y
  142.643 en `2025-11`. El Gansito subió **+16.47% real**, más del doble de la inflación general.
- **El bootstrap remuestrea tiendas como unidad de muestreo**, pero el diseño de Profeco no es
  aleatorio: eligen a quién visitar y el criterio no lo publican, citado en
  [`fuentes.md`](fuentes.md). El intervalo aproxima la variabilidad, no es inferencia sobre
  todas las tiendas de México. Va impreso junto al número.
- **Gold reproduce las cifras, calculadas sobre `hechos_relativos` con la agregación que hará
  la medida DAX** —promedio dentro del eslabón, suma entre eslabones, exponencial—: +24.40%
  nominal, +16.47% real y +6.81% de inflación, los tres al centésimo contra lo medido en la
  zona raw, y por canal +29.84% supermercado, +11.35% conveniencia y +5.16% tradicional. El
  pareo mínimo por canal es 135, 31 y 7: **tradicional no alcanza el umbral de 30 y por eso no
  publica índice**, que es la guarda funcionando sobre datos reales y no sobre un supuesto.
- **El pareo cuesta 30 tiendas de 508.** `hechos_relativos` toca 478 tiendas y no las 508 que
  venden Gansito alguna vez: las otras 30 nunca aparecen en dos quincenas consecutivas, así que
  no forman ningún eslabón. Son 12,393 pares en 45 eslabones, 275 de media.

- **Gold reproduce el intervalo, no sólo la cifra.** El bootstrap de `nb_30_gold`, con otra
  semilla, da un EE de **1.6027 pp** contra el 1.60 de la exploración, y un IC de
  **+21.25% a +27.65%** contra el +21.36 – 27.64 medido con polars. La diferencia de 0.11 pp en
  la punta baja es ruido de Monte Carlo, y es la señal de que se reproduce el mismo bootstrap.
- **El Gansito encabeza su propia canasta por mucho.** Con la misma metodología y las mismas
  tiendas, el cambio encadenado de los nueve SKUs va de +24.40% a +1.16%, y el segundo queda
  nueve puntos abajo:

  | SKU | cambio | SKU | cambio |
  |---|---|---|---|
  | Gansito | +24.40% | Chocoroles | +8.69% |
  | Nito | +15.13% | Pingüinos | +7.80% |
  | Mantecadas | +9.49% | Roles de Canela | +3.81% |
  | Panqué Nuez | +8.99% | Donitas Espolvoreadas | +1.16% |
  | Panqué con Pasas | +8.74% | | |

  Los nueve pasan la guarda con holgura —el pareo mínimo va de 162 a 206 contra un umbral de
  30—, así que ninguna barra se apaga.
- **El intervalo de la canasta no sirve como intervalo del Gansito.** Tabulado por quincena sin
  separar SKU, el bootstrap da ±0.37 pp y un cambio de +10.5%: promedia nueve series y sale
  cuatro veces más angosto. Es la razón de que `hechos_ic_indice` tenga grano SKU × quincena
  (decisión #20).
- **`BLANK() - 1` es `-1` en DAX, y eso mordió a cuatro medidas.** Restar antes de guardar
  convierte "no hay dato" en un número: `Intervalo del cambio` publicaba "-100.00%",
  `Inflación acumulada %` devolvía -100% sin contexto de mes y arrastraba `Índice INPC` a 0
  —un desplome dibujado que no existe en los datos—, y `Cambio real %` publicaba **0.00%** en el
  corte que la guarda de pareo había dejado sin índice, que es el caso peor porque 0.00% parece
  un dato. La guarda va antes de la resta, siempre.
- **La quincena base no produce eslabón y el índice vale 100 ahí por definición.** La guarda de
  pareo la apagaba —no hay pareo que medir contra una quincena anterior que no existe— y la
  serie arrancaba en blanco un eslabón tarde.
- **Ninguna cadena alcanza el umbral de pareo.** La mejor es **Hipermercado Soriana con 26**
  tiendas pareadas en su peor eslabón, y detrás Wal-mart 20, Chedraui 16 y Bodega Aurrera 15;
  las otras 33 no pasan de 8. Con el umbral en 30, el cambio encadenado por cadena sale en
  blanco las 37 veces (decisión #25).
- **`canal` es nulo para 1,429 de las 2,392 tiendas del padrón**, las de los giros que no venden
  pastelillos —tortillerías 756, papelerías 143, electrodomésticos 143, pescaderías 74,
  panaderías 73, departamentales 72, uniformes 58, jugueterías 45, vinaterías 42, zapaterías
  23—, y **ninguna tiene una sola celda** en `hechos_precios`. El diccionario mapea los cinco
  giros que sí venden y deja el resto sin canal, que es lo correcto y lo que obligó a la
  decisión #27.
- **El tramo completo por cadena, con cinco tiendas en las dos puntas: 12 cadenas, y todas
  suben.** Es lo que dibuja el dumbbell de P3. Los que arrancaron baratos son los que más
  subieron, con la excepción del mercado tradicional, que arrancó barato y se quedó barato:

  | cadena | inicio | cierre | cadena | inicio | cierre |
  |---|---|---|---|---|---|
  | Chedraui | $14.25 | $21.27 | Hipermercado Soriana | $16.01 | $20.26 |
  | Bodega Aurrera | $14.90 | $20.67 | Mercado Publico | $16.02 | $18.31 |
  | Wal-mart | $15.24 | $21.59 | Mega Soriana | $16.27 | $20.25 |
  | H.e.b. | $15.78 | $19.90 | Farmacia Guadalajara | $18.41 | $21.36 |
  | Central de Abastos | $15.80 | $17.75 | Ley | $19.94 | $22.15 |
  | Soriana Super | $15.94 | $20.72 | Oxxo | $19.98 | $22.58 |

  El promedio nacional del mismo tramo va de **$16.58 a $20.83**. Superissste queda fuera de las
  doce por la punta de arranque: en `2024-01_q1` no vendía Gansito en ninguna tienda.
- **La brecha entre dos puntas no se publica, porque no es una cantidad estable.** Parando en
  `2025-10_q2` da −$0.60, en `2025-11_q1` $0.40 y en `2025-11_q2` $1.30, y esta última trae la
  muestra más chica de conveniencia (36 tiendas): la misma serie sostiene tres conclusiones
  distintas según dónde se corte. Las cifras viejas de $4.16 y $0.94 no salen de ninguna
  población ni agregación —se probaron celdas observadas ($3.49 / $1.30, que es lo que da el
  modelo), sólo pareadas, las dos puntas, panel balanceado, ponderada por visitas, mediana,
  media geométrica, la canasta completa y otras parejas de puntas—: vienen de una exploración
  cuyos filtros ya no se conocen.
- **Un día de salario mínimo compraba 15.6 Gansitos y compra 13.9.** El mínimo nacional pasó de
  $258.80 a $289.75 diarios, **+11.96%**, contra el +24.40% del Gansito: la caída del poder
  adquisitivo es −10.88% medida sobre los niveles y −10.00% sobre las dos series base 100
  (decisión #29).
- **El salario mínimo es escalón, no serie.** CONASAMI lo fija una vez al año, así que
  `Índice salario mínimo` es plano en 100 las 24 quincenas de 2024, salta a **111.96** en
  `2025-01_q1` y ahí se queda las 22 restantes. Contra un precio que sube todo el año, los
  Gansitos por día bajan de 15.6 a un piso de **13.1** en `2024-11_q2`, rebotan a 14.8 con el
  escalón de enero y vuelven a bajar a 13.9. El máximo de la serie es la quincena base.

## El corte transversal

Medido con `executeQueries` sobre el modelo, para la última quincena —`2025-11_q2`— y el
Gansito, que es como dejan el contexto los slicers de P2.

- **220 tiendas y 316 visitas**, promedio **$20.83**, mediana **$21.50**, de **$16.00** a
  **$26.00**, con p90/p10 en **1.28**.
- **El precio es de anaquel y no continuo.** 39 precios distintos en $10 de rango, y uno domina:
  **58 de las 220 tiendas cobran exactamente $22.00**, 31 cobran $18.90 y 20 cobran $18.00. Con
  bins de un peso son once barras —3, 3, 23, 36, 22, 15, **79**, 30, 5, 2, 2—, y el pico se ve
  de un vistazo.
- **37 cadenas, y la mayoría trae de una a tres tiendas.** Con cinco o más quedan 13, de **Oxxo
  a $22.58** hasta **Central de Abastos a $17.75**: casi $5 por el mismo pastelito. Sin ese
  mínimo la cadena más cara es una farmacia con dos tiendas a $26.
- **30 estados en todo el padrón de 2,392 tiendas.** Colima y Nayarit no aparecen nunca, así que
  el mapa los deja sin pintar con razón. En la última quincena van de **Durango $22.82** a
  **Veracruz $19.38**.
- **Los nombres de municipio se repiten entre estados.** "Benito Juárez" junta el de Ciudad de
  México con el de Quintana Roo, que cobran **$20.45 y $21.57**: agregados por nombre dan una
  fila de 11 tiendas a un precio que no existe en ningún lado.
- **Reconstruir `dim_tienda` no movió el índice.** Después del drop y la corrida de `nb_30` el
  encadenado sigue en +24.40%, el real en +16.47% y el intervalo en +21.25% a +27.65%: las
  llaves son `xxhash64` deterministas, así que los hechos volvieron a empatar con las mismas
  filas.

## La copia pública

- **Gold entero pesa 0.87 MB en ocho parquets.** Eran 1.36 MB antes de soltar la partición y
  prender el V-Order: `hechos_precios` bajó de **771,008 a 378,767 bytes** (−50.9%) y
  `hechos_relativos` de **327,677 a 224,395** (−31.5%). Las otras seis salieron **idénticas al
  byte**, que es la señal de que el cambio está donde se tocó y en ningún otro lado.
- **El `coalesce(1)` de `nb_50_export` ya no puede argumentar compresión.** Cuando la tabla eran
  46 archivos, fusionarlos al exportar ahorraba un tercio; ahora la tabla ya viene en uno y el
  parquet exportado pesa 378,767 contra los 379,918 de la tabla. Sigue haciendo falta, pero por
  la otra razón: que la URL que consume Power BI tenga nombre fijo y no un `uuid` por corrida.
- **El repo de datos queda en 7.5 MB** —6.6 de Profeco, 872 KB de la copia pública y 44 KB de
  CONASAMI—, contra el techo blando de **1 GB** que GitHub recomienda y los **50 MiB** por
  archivo donde apenas avisa. El parquet más grande es el 0.7% de ese umbral.
  Git guarda cada versión y el parquet no hace delta, así que cada reexport son ~1.4 MB nuevos:
  cien reexports serían 200 MB.
- **Los parquets reproducen el índice sin Fabric de por medio.** Leyéndolos con polars y sumando
  los log-relativos por quincena, el cambio encadenado del Gansito da **+24.40%**, la misma
  cifra que publica el modelo. Traen todas las columnas, incluida la `_quincena` oculta, así que
  el TMDL del modelo import se copia sin renombrar nada.
- **`fab cp` baja archivos de `Files` a disco local**, no sólo entre rutas de Fabric: avisa
  "not found in Fabric, checking local file system" y copia. Es la última milla de gold a git
  mientras no haya workflow.

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
