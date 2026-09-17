# La plataforma: lo que hace, verificado

Cómo se comportan Fabric, Delta, Spark, Power BI, la git integration y la CLI. Cada punto se
probó una vez, cambia cómo se escribe el código, y no depende de los datos: nada de aquí caduca
con la ventana de la serie. Donde la doc lo dice, va el enlace; donde no lo dice, se dice que se
midió.

Lo que se eligió a partir de esto está en [`decisiones.md`](decisiones.md); lo que se midió sobre
los datos, en [`mediciones.md`](mediciones.md).

Los dos workspaces corren **Runtime 2.0**: Spark 4.1.1, Python 3.13.11, Delta 4.2.

## Runtime y Delta

- **Fabric deja ANSI apagado**, contra el default de Spark 4. Se prende por sesión con
  `spark.conf.set("spark.sql.ansi.enabled", "true")`; con eso `cast` truena y `try_cast` sigue
  dando nulo (decisión #16).
- **Las deletion vectors vienen prendidas por defecto**, así que una tabla nueva nace en el
  protocolo (3,7) con `deletionVectors` y `delta.targetFileSize.adaptive`. Un runtime más viejo no
  la lee (decisión #11).
- **`SHALLOW CLONE` es zero copy de verdad.** Funciona por ruta `abfss` sin lakehouse por defecto,
  entre lakehouses y entre workspaces: `_delta_log` sin un parquet propio, y el origen intacto
  después de escribirle al clon. El clon hereda el protocolo del origen. Sostiene la decisión #5.
- **El clon por ruta no reemplaza: hay que borrar el destino.** `CREATE OR REPLACE TABLE … SHALLOW
  CLONE` truena con `DELTA_UNSUPPORTED_NON_EMPTY_CLONE` en cuanto el destino tiene filas. `nb_91`
  borra el directorio con `notebookutils.fs.rm` y clona limpio: dev arranca en la v0 de prod. Sólo
  desaparece lo que dev escribió; los parquets son de prod y el clon nunca los tuvo.
- **Lo que rompe un clon es `OPTIMIZE` seguido de `VACUUM`**: el primero deja huérfanos los
  archivos que el clon referencia y el segundo los borra. `VACUUM` solo no lo rompe.
- **Las constraints CHECK funcionan sobre tabla por ruta.** `ALTER TABLE delta.\`abfss://…\` ADD
  CONSTRAINT` engancha sin lakehouse por defecto, y las puestas salen en
  `DeltaTable.forPath(…).detail()["properties"]` como `delta.constraints.<nombre>`, que es lo que
  las hace idempotentes. El `ADD` valida lo que ya está en la tabla.
- **`CLUSTER BY` también engancha por ruta**, y el clustering sobrevive las escrituras
  posteriores. Pero **`DataFrameWriter.clusterBy` es un no-op silencioso sobre tabla por ruta**:
  el método existe, no truena, y `DESCRIBE DETAIL` devuelve `clusteringColumns` vacío. Por eso el
  layout se declara con un `ALTER` idempotente (decisión #32).
- **`replaceWhere` no exige que la columna del predicado sea de partición, y el guard sigue
  vivo**: una fila fuera del predicado truena con `DELTA_REPLACE_WHERE_MISMATCH`. Un
  `replaceWhere` vacío no commitea versión; una quincena inventada truena antes de escribir.
- **Recalcular una quincena no reescribe el archivo grande: lo marca con una deletion vector** de
  cientos de bytes, y escribe las filas nuevas en un archivo aparte. Soltar la partición no trajo
  el write amplification que se temía. La reescritura es determinista: dos corridas idénticas
  dan archivos con el mismo `md5`.
- **La DV acumulada es el trabajo del mantenimiento.** Las filas marcadas se quedan hasta que
  alguien purgue; `OPTIMIZE` purga solo a partir del 5 % de la tabla.
- **Un MERGE que no cambia nada no commitea versión.** Sus métricas se leen comparando la versión
  de antes contra la de después, no mirando la última entrada del log.
- **Con `readHeavyForPBI`, el V-Order entra como un commit de Delta aparte**, detrás de cada
  escritura que commitea: `operation = 'AUTOSET VORDER TBLPROPERTY'` con `operationMetrics` vacío,
  unos cientos de milisegundos después. Rompe cualquier lectura que dé por hecho que `history(1)`
  es la escritura propia (`upsert` de `nb_00_config` tronaba con `KeyError`). El perfil documenta
  V-Order, optimize write y bin de 1 GB, y no menciona este commit
  ([perfiles](https://learn.microsoft.com/fabric/data-engineering/configure-resource-profile-configurations#default-configuration-values-by-profile)).
- **El perfil de recursos del workspace decide V-Order y optimize write, y el default es
  `writeHeavy`**, que trae V-Order apagado y no deja puesto el interruptor de optimize write. Los
  perfiles `readHeavy*` sí los prenden. **Puesto en sesión, el perfil aplica lo que la doc dice**:
  `spark.conf.set("spark.fabric.resourceProfile", "readHeavyForPBI")` mueve
  `spark.sql.parquet.vorder.default` a `true` y deja `optimizeWrite.enabled` en `true` con
  `binSize` de `1g`. Lo verificado es la sesión, no los bytes: el V-Order no se ve en el metadato
  de la tabla.
- **Cuánto vale cada cosa**, medido sobre `hechos_precios` con un archivo de control:

  | escritura | archivos | tamaño relativo |
  |---|---|---|
  | sin V-Order | 1 | 100 % |
  | con V-Order | 1 | **46 %** |
  | fuente ya V-Ordenada, reescrita sin V-Order | 1 | 46 % |
  | esa misma, particionada por `_quincena` | 46 | 100 % |

  El V-Order vale la mitad y particionar cuesta el doble, y son la misma mecánica: qué tan bien
  se agrupan las filas parecidas dentro de un archivo. El tercer renglón separa las dos cosas: el
  orden que impone el V-Order sobrevive una reescritura sin V-Order, así que lo que se pierde al
  reescribir sin él son unos puntos de encoding, no el ordenamiento.
- **`OPTIMIZE … VORDER` no V-Ordena hacia atrás lo que decide no reescribir.** Sobre archivos
  chicos escritos sin V-Order: todos considerados, todos saltados, `NonVOrderedFiles` en cero. Por
  eso el V-Order entra por el perfil de la escritura y el `VORDER` del mantenimiento sólo lo
  conserva (decisión #33).
- **`OPTIMIZE` no commitea versión cuando no reescribe nada, y `VACUUM` commitea dos siempre**
  (`VACUUM START` y `VACUUM END`: Fabric trae prendido el logging del vacuum). Cada mantenimiento
  deja dos versiones nuevas en cada tabla aunque no se haya movido un dato, y eso desframea al
  modelo Direct Lake igual. Es otra razón para que el refresh vaya dentro del pipeline.
- **El `DRY RUN` del `VACUUM` lista el `metadata/` de Iceberg en todas las tablas, y el `VACUUM`
  no lo borra.** Fabric mantiene por tabla un directorio con la virtualización Iceberg de OneLake,
  y el listado lo toma por candidato. `nb_60` lo descuenta de su conteo.
- **`VACUUM … LITE` combina con `DRY RUN`, y aquí no sirve**: arma la lista leyendo el log en vez
  del directorio, y se le escapan huérfanos reales que el modo completo sí borra.
- **`clusteringQuality` viene dentro de las métricas de `OPTIMIZE`, en PySpark**, aunque la doc lo
  presente como método de Scala: una fila por columna de clustering, con `avgDepth`,
  `overlapRatio` y `skippingEffectiveness`.
- **OneLake no acepta mezclar GUID y nombre en `abfss://`**: 400 `FriendlyNameSupportDisabled`.
  Los dos segmentos van por GUID; ver `ruta_tabla` en `nb_00_config`.
- **Spark no habla HTTPS**: los bytes bajan con `requests` al driver. La capacidad sí sale a
  `raw.githubusercontent.com`, con texto y con binario.
- **`notebookutils.fs.ls` distingue vacío de inexistente**: sobre `Tables/dbo` da una entrada por
  tabla; sobre una ruta que no existe truena con 404. `notebookutils.fs.exists` distingue sin
  tronar. Por eso `nb_91` enumera el bronze de prod en vez de llevar una lista, y un cero es "prod
  está vacío" y no "me equivoqué de ruta".
- **Un workspace se resuelve por nombre con `sempy`, no con notebookutils**:
  `fabric.resolve_workspace_id(nombre)` da el GUID y truena con `WorkspaceNotFoundException` si no
  existe. `notebookutils.lakehouse.get` acepta otro workspace, pero por GUID.

## Sesiones de Spark y pipelines

- **La capacidad de trial no aguanta dos sesiones de Spark a la vez.** Dos actividades disparadas
  en el mismo segundo: una consiguió sesión de Livy y la otra se fue con
  `430 TooManyRequestsForCapacity`. Dev y prod comparten la capacidad, así que conviene cerrar las
  sesiones interactivas de dev antes de correr prod.
- **La trial no encola: el 430 es rechazo, no espera.** La cola de jobs existe en SKUs F y P, no en
  trial ([job queueing](https://learn.microsoft.com/fabric/data-engineering/job-queueing-for-fabric-spark#queue-sizes)).
- **El 430 entre dos notebooks encadenados es intermitente, no un rezago fijo.** Con el mismo
  intervalo entre actividades, una corrida se fue con 430 y otra entró a la primera: depende de qué
  más esté consumiendo la capacidad. Por eso la red dentro de un pipeline es el `retry` de la
  actividad y no un `Wait` (decisión #10).
- **El `sessionTag` no basta para compartir sesión: hace falta el switch del workspace.** El tag
  sólo agrupa; quien convierte las sesiones de pipeline en alta concurrencia es *Spark settings →
  High concurrency → For pipeline running multiple notebooks*. Con el tag y el switch apagado,
  `sessionId` distintos y `highConcurrencyModeStatus` en `null`; prendido, las actividades
  comparten `sessionId` y el estado dice `"sessionSource": "created"` en la primera y
  `"attached"` en las demás. Los docs no dicen el estado inicial del switch; en estos workspaces
  venía apagado (`entorno.md`).
- **Compartir sesión no acelera nada medible, pero elimina el 430** entre las actividades que
  comparten: la segunda deja de pedir sesión.
- **El límite de 5 notebooks por sesión de alta concurrencia se sube sólo desde un Environment**
  ([high concurrency](https://learn.microsoft.com/fabric/data-engineering/high-concurrency-overview#dynamic-session-sharing-limit)).
  Sostiene la decisión #41.
- **La sesión sobrevive a su pipeline entre uno y dos minutos** (medido con `livySessions`: de 68
  a 101 s entre el fin del pipeline y el `endDateTime`). No es el timeout de inactividad, que son
  20 minutos. El primer notebook del pipeline siguiente pide sesión unos 25 s después de arrancar,
  así que dos pipelines encadenados sin espera chocan. Con un `Wait` de 120 s entre invokes entran
  con margen (decisión #41).
- **Con High concurrency, el perfil de recursos que puso un notebook sigue puesto en el
  siguiente.** Por eso cada notebook que escribe declara el suyo (decisión #33).
- **Los base parameters de la actividad de notebook sólo llevan escalares** (`int`, `float`,
  `bool`, `string`); `list` y `dict` no, y la doc recomienda serializar a JSON
  ([doc](https://learn.microsoft.com/fabric/data-engineering/author-execute-notebook#integrate-a-notebook)).
  Por eso `quincenas_pedidas` es una cadena.
- **Ninguna API pública devuelve el exit value de un notebook**: ni job instances ni livySessions;
  se queda en el snapshot. Para leerlo desde la CLI, el notebook escribe el mismo JSON a `Files/`
  y se baja con `fab cp`.
- **`activityId` es de la sesión, no de la ejecución.** Dos corridas seguidas en la misma sesión
  interactiva, y las dos actividades de un pipeline con sesión compartida, salen con el mismo
  `corrida`. No sirve de llave para una tabla de corridas (decisión #31). El id por ejecución
  existe del lado del pipeline: el `runId` de cada actividad.
- **Un pipeline sale verde o rojo por sus actividades finales.** Si la última se saltó, cuenta la
  anterior ([error handling](https://learn.microsoft.com/azure/data-factory/tutorial-pipeline-failure-error-handling#error-handling)).
  Con `Completed` antes de la última, un fallo intermedio queda verde si la final pasa. Y una
  actividad que falla con sólo camino de completion se da por manejada (decisión #10).
- **Los pipelines invocados con *Invoke pipeline (Legacy)* no aparecen en su propio historial.**
  `fab job run-list` del hijo no lista las corridas que dispara el padre; se leen con
  `queryactivityruns` sobre la corrida del padre, cuyo `output.pipelineRunId` lleva a las
  actividades del hijo. Al commitear, el `referenceName` del invoke queda como `logicalId`, así
  que no pasa por `parameter.yml`.
- **El monitoring hub retiene 30 días de actividades y 60 el historial de notebook**, snapshot
  incluido ([hub](https://learn.microsoft.com/fabric/admin/monitoring-hub),
  [límites](https://learn.microsoft.com/fabric/data-engineering/notebook-limitation#other-specific-limitations)).
  Sostiene la decisión #31.
- **Los avisos de fallo viven en *Schedule failures*, y son de items programados**: no hay nada
  que prender hasta que el pipeline tenga schedule (`entorno.md`).
- **Un schedule guardado apagado se commitea con `"enabled": false`**, y `.schedules` no lleva los
  avisos de falla. Fabric exige fecha de fin; sin ella escribe `9999-12-31`
  ([pipeline runs](https://learn.microsoft.com/fabric/data-factory/pipeline-runs#scheduled-pipeline-runs)).
  La zona `Central Standard Time` es la de Estados Unidos, con horario de verano; la de México es
  `Central Standard Time (Mexico)`.
- **La variable library no tiene tipo arreglo y su value set activo no viaja en el item**
  ([doc](https://learn.microsoft.com/fabric/cicd/variable-library/variable-library-overview),
  [value sets](https://learn.microsoft.com/fabric/cicd/variable-library/value-sets)). **Una user
  data function es Python serverless sin Spark**, por REST, con cooldown de publicación
  ([doc](https://learn.microsoft.com/fabric/data-engineering/user-data-functions/user-data-functions-overview)).
  Sostienen la decisión #18.

## Capacidad y facturación

- **Una capacidad de pago por uso se cobra por minuto mientras está activa**
  ([cost optimization](https://learn.microsoft.com/azure/well-architected/microsoft-fabric/cost-optimization#identify-key-cost-drivers)),
  y pausarla detiene los medidores de cómputo de todos los workloads
  ([effect on billing](https://learn.microsoft.com/fabric/data-warehouse/pause-resume#effect-on-billing)).
- **El almacenamiento de OneLake se sigue cobrando con la capacidad pausada**, y mientras lo esté
  se rechaza toda transacción contra ella
  ([OneLake consumption](https://learn.microsoft.com/fabric/onelake/onelake-consumption)). Es el
  único cargo del proyecto que no baja a cero.
- **Al pausar, el remanente de operaciones suavizadas y de overage se suma a la factura**
  ([pause and resume](https://learn.microsoft.com/fabric/enterprise/pause-resume)). Meter una
  corrida en una ventana más corta no la abarata por sí sola: lo que exceda la capacidad se cobra
  por su propio medidor, más caro que el cómputo base.
- **Los precios no se escriben aquí.** El catálogo vigente se consulta con la Retail Prices API
  filtrando por `serviceName`, que es lo que indica la propia doc de facturación de Fabric: la
  página de precios no enumera los medidores
  ([lista de medidores](https://learn.microsoft.com/fabric/enterprise/azure-billing#get-the-current-list-of-meters)).
  Key Vault standard no cobra por existir, cobra por operaciones.

## Identidades y permisos

- **Un notebook dentro de un pipeline corre como quien modificó el pipeline al último**, no como
  el dueño del pipeline ni el del notebook
  ([security context](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook#security-context-of-running-notebook)).
  Un service principal que actualiza la definición por la API pasa a ser ese `LastModifiedBy`
  ([set pipeline owner](https://learn.microsoft.com/fabric/data-factory/set-pipeline-owner-tutorial)).
  En prod los notebooks corren con la cuenta del despliegue; en dev, con quien editó el pipeline
  en el UI. Lo que un notebook hace en prod no se prueba desde dev. Con qué identidad corrió se
  ve en el `submitter` de las sesiones del notebook
  ([livy sessions](https://learn.microsoft.com/rest/api/fabric/notebook/livy-sessions/list-livy-sessions)):
  en la corrida programada de `pl_master` en prod aparece `sp-indice-gansito-deploy`.
- **Contributor es el mínimo que documenta `fabric-cicd`**
  ([PBIP con fabric-cicd](https://learn.microsoft.com/power-bi/developer/projects/projects-deploy-fabric-cicd#prerequisites))
  y alcanza para escribir en un lakehouse. Admin agrega borrar el workspace y administrar accesos
  ([permission model](https://learn.microsoft.com/fabric/security/permission-model)). El
  `ReadWrite` de OneLake security acotaría más, pero está en preview y no dice si alcanza para
  borrar un directorio. Con Contributor también se publican los `.schedules`.
- **Publicar un pipeline que usa una conexión exige acceso a esa conexión.** Sin él, el
  despliegue truena con `User does not have access to the connection used in the Pipeline` y
  arrastra a lo que invoque ese pipeline. Una conexión creada por un usuario se comparte con el
  service principal, con rol User
  ([how it works](https://learn.microsoft.com/fabric/cicd/git-integration/automate-git-integration-with-service-principal#how-it-works)).
  Compartida así, también le alcanza para correr el refresh de `sm_gansito` desde el pipeline.
- **`sempy.fabric.resolve_workspace_id` funciona con service principal**: está en la lista
  soportada ([semantic link + SPN](https://learn.microsoft.com/fabric/data-science/semantic-link-service-principal-support#supported-semantic-link-functions)).
  De `notebookutils.fs` y `notebookutils.lakehouse` los docs no dicen nada: se ve en la primera
  corrida de `pl_mantenimiento` en prod.
- **`notebookutils.credentials.getSecret` lee Key Vault con la identidad de quien corre el
  notebook**, y hace falta permiso de lectura sobre el secreto
  ([get secret](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-credentials#get-secret)).
  Que funcione con service principal no lo dice: se ve en la primera corrida de `pl_gold` en prod.
- **`notebookutils.runtime.context["currentWorkspaceName"]` está en todos los contextos**,
  interactivo y pipeline
  ([runtime context](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-runtime#view-session-context)).
- **Un usuario B2B no puede ser capacity administrator**, y el admin tiene que pertenecer al
  tenant donde se aprovisiona la capacidad
  ([buy capacity](https://learn.microsoft.com/fabric/enterprise/buy-capacity#buy-an-azure-capacity-sku-for-fabric)).
- **Pausar y reanudar una capacidad son permisos de Azure, no de Fabric**: piden
  `Microsoft.Fabric/capacities/suspend/action` y `resume/action` sobre el recurso
  ([pause and resume](https://learn.microsoft.com/fabric/enterprise/pause-resume)). Ser capacity
  admin no alcanza, y al revés tampoco: quien prende la capacidad y quien corre los notebooks
  pueden ser dos cuentas distintas, y aquí lo son (`entorno.md`).

## Git integration y despliegue

- **La git integration reescribe las referencias entre items al commitear.** En una actividad de
  notebook, el `workspaceId` queda como GUID nulo ("el workspace donde corro") y el `notebookId`
  como el `logicalId` del notebook; `fabric-cicd` los resuelve al publicar porque los notebooks van
  en el mismo despliegue y antes que el pipeline. El JSON que muestra la UI no es el que se
  despliega: ahí los dos campos son GUIDs literales del workspace vivo.
- **Tres cosas no se convierten y pasan por `parameter.yml`.** La actividad *Semantic model
  refresh* llega a git con el `groupId` y el `datasetId` de dev, y la conexión como
  `externalReferences.connection`; se reemplazan por JSONPath hacia `$workspace.$id` y
  `$items.SemanticModel.sm_gansito.$id`
  ([parameterization](https://microsoft.github.io/fabric-cicd/latest/how_to/parameterization/)).
  El Direct Lake de `sm_gansito` (`expressions.tmdl`) guarda el workspace y el `lh_gold` de dev
  literales; dos `find_replace` por regex los cambian por `$workspace.$id` y
  `$items.Lakehouse.lh_gold.$id`, que resuelve aunque Lakehouse no esté en alcance. Y los
  `.schedules` se parametrizan como cualquier archivo: `true` en prod, `false` en dev. Todo
  validado con `validate_parameter_file` y simulado con las funciones de la librería.
- **Fabric escribe `.platform` sin salto de línea final.** Con uno de más, el item sale
  `uncommitted` al sincronizar aunque el contenido sea idéntico. El `notebook-content.py` escrito
  a mano sí sobrevive el round-trip, celda `%run` incluida.
- **El índice de git guarda LF y el árbol de trabajo se ve en CRLF**, por `core.autocrlf`, que en
  Windows viene en `true` desde el gitconfig del sistema (se consulta con `--show-origin`). Lo que
  dice la verdad es `git ls-files --eol`.
- **El fallo real es el blob con finales mezclados.** Editar línea a línea un archivo que en el
  árbol está en CRLF mete líneas con LF solo, y un blob mezclado envenena todos los diffs
  siguientes: un commit marcaba cientos de líneas donde sólo decenas eran contenido (medido con
  `git diff --ignore-cr-at-eol`). Se arregla con `.gitattributes` en `* text=auto` y `git add
  --renormalize .`, que no toca una línea de contenido.
- **La git integration corta el mensaje del commit cerca de los 300 caracteres**, cuerpo incluido
  y sin avisar. Los commits que salen del UI se escriben de una línea; el porqué largo va al
  comentario del notebook o a estos documentos.
- **Los workflows sólo se registran desde la rama por defecto**: `workflow_dispatch` y `schedule`
  no existen mientras el archivo viva sólo en `dev`.
- **`fabric-cicd` corre en Python 3.11**, pinneado en `deploy.yml` con `--no-project`: ahí manda lo
  que la herramienta soporte, no el runtime de Fabric.
- **El manifiesto es el estado del pipeline, y `main` es lo que corre.** Mover la zona raw de
  directorio mientras el script de `main` seguía leyendo el viejo dejó al cron rehaciendo
  quincenas en el layout anterior. Mover el estado de una fuente y su productor va junto, y en el
  mismo merge a `main`.

## La CLI de Fabric

`fab`, autenticada contra `app.fabric.microsoft.com`. Se instala con `uv tool install --python
3.12`; con 3.14 truena con pyyaml.

- **`--format .py` no es opcional, y va en los dos sentidos.** Sin él, `fab export` baja un
  `.ipynb` y `fab import` espera JSON: falla con `InvalidNotebookContent`. Con él, el archivo es el
  mismo `notebook-content.py` de git.
- **`fab import` crea y actualiza con el mismo comando**; `-f` sólo se salta la confirmación. El
  directorio de salida de `fab export` tiene que existir (`InvalidPath` si no).
- **El `.platform` que baja la CLI no es el de git**: trae `logicalId` en ceros y otra sangría. No
  se copia al repo; el `notebook-content.py` sí. **Y el `logicalId` que se sube tampoco importa**:
  al commitear desde la UI, la git integration escribe otro. Hay que poner uno bien formado, no uno
  en particular.
- **`fab export` no es fiel byte por byte, y lo parece.** Cuando lo guardado está en CRLF, lo que
  baja trae los CR duplicados (`CR CR LF`), así que un `diff` contra el archivo local marca todas
  las líneas. Para verificar qué cambió, el panel de código fuente de la UI y no el export.
- **Un import contra un notebook abierto en el UI no se pierde ni pisa en silencio**: al guardar,
  la pestaña avisa y deja elegir versión. Elegir es todo o nada, no un merge por celda.
- **`fab import` de un `Report` exige `byConnection`.** El `definition.pbir` de git apunta al
  modelo con `byPath`, y con eso el import falla con `InvalidDefinitionPayload`. Lo que baja `fab
  export` ya trae `byConnection`; el repo conserva `byPath`.
- **`fab import` de un `Report` reemplaza sus recursos estáticos por los de la carpeta.** Subir una
  definición sin `StaticResources` borra el tema del item y deja `report.json` apuntando a un
  recurso que ya no existe, sin avisar.
- **El TMDL se sube con los finales de línea del item**, que son CRLF. Se normaliza el archivo
  entero antes de subirlo (ver el blob mezclado, arriba).
- **`fab api` antepone `https://api.powerbi.com/v1.0/myorg/`** con `-A powerbi`: el endpoint se
  escribe `datasets/<id>/executeQueries`. El cuerpo se lee con la codificación del sistema (cp1252
  en Windows), así que el JSON va en ASCII con escapes unicode o truena con `charmap codec can't
  decode`.
- **La exploración no necesita notebook.** `fab api -A powerbi -X post
  datasets/<id>/executeQueries` corre DAX contra el modelo, y `fab job run` corre un notebook y
  espera. **`fab cp` baja archivos de `Files` a disco local.**

## Power BI y Direct Lake

- **Direct Lake exige capacidad; un modelo import en My Workspace no.** *Publish to web* desde My
  Workspace pide una licencia de Power BI, no Pro, y que un admin encienda el setting del tenant.
  No admite DirectQuery, live connection, RLS ni medidas a nivel reporte
  ([publish to web](https://learn.microsoft.com/power-bi/collaborate-share/service-publish-to-web#prerequisites)).
- **En capacidad compartida caben ocho refresh programados al día**, y el programa se pausa solo
  tras dos meses sin que nadie abra el reporte
  ([scheduled refresh](https://learn.microsoft.com/power-bi/connect-data/refresh-scheduled-refresh#scheduled-refresh)).

- **Direct Lake no admite columnas calculadas.** Eso descarta el binning del UI y cualquier
  `SWITCH` sobre una columna de una tabla Direct Lake: lo derivado se calcula en gold (decisiones
  #21, #22, #24, #30).
- **Tablas calculadas de constantes: sí**, mientras no referencien una tabla Direct Lake.
  `Medidas`, `Umbral de pareo`, `Rango de precio` y `Nivel de cobertura` son eso.
- **Una definición que cambia no se procesa sola.** Una tabla nueva subida por import, y también
  una columna nueva en una tabla que ya existía, responden `Cannot find table` o `columna no
  encontrada` hasta un refresh: `fab api -A powerbi -X post datasets/<id>/refreshes` con
  `{"type":"Full"}`.
- **El *unknown member* se queda en Direct Lake.** Cada relación regular agrega una fila virtual
  del lado "uno" al expandir con `LEFT OUTER JOIN`, y sin validar integridad contra los Delta no
  desaparece: cada dimensión cuenta un miembro más que sus filas. *Assume referential integrity*
  la quita cambiando el join a `INNER` (decisión #28).
- **`BLANK() - 1` es `-1` en DAX.** Restar antes de guardar convierte "no hay dato" en un número;
  mordió a cuatro medidas de una vez, y una publicaba `0.00%` donde la guarda había dejado el corte
  sin índice. La guarda va antes de la resta, siempre.
- **El título automático de un visual dice "X by Y", en inglés**, porque el modelo declara
  `culture: en-US`. Se pone `text` explícito en `visualContainerObjects.title`.
- **Varias columnas en `Category` son una jerarquía, no un eje compuesto.** Con `anio`, `mes` y
  `quincena` activos el gráfico arranca en el nivel superior y dibuja un punto por año.
- **`visualInteractions[].type` es `NoFilter`.** `NoEffect` lo rechaza el esquema. Hace falta
  cuando un visual sin filtro convive con otros que sí lo tienen: el filtrado cruzado se suma al
  filtro de visual y la intersección vacía deja la página en blanco de un clic.
- **El mapa integrado de México viaja como recurso estático** `mexico.states.topo`, referenciado
  con un `ResourcePackageItem`: el tipo de mapa es parte del item, no una propiedad.
- **El slicer de un what-if pide 102 de alto**, no los 78 de la fila de tarjetas: con
  `objects.data.mode` en `'Single'` dibuja el deslizador debajo de la caja. El valor por defecto
  no sobrevive escrito como `In` de un literal entero: el UI lo reescribe a `Comparison` con
  `ComparisonKind: 0` contra un literal `D`, y lo baja al mínimo de la serie.
- **La banda de confianza es nativa**: `objects.error` con `errorRange.explicit`, `isRelative:
  false` y las medidas en `lowerBound`/`upperBound`. Con muchos puntos, `shadeShow: true` y
  `barShow`/`markerShow` en `false`: las barras por punto tapan el relleno.
- **`textStyle` de un textbox acepta `fontColor`**, además de tamaño, peso, estilo y decoración:
  la paleta se escribe por PBIR sin pasar por el UI.

## Power BI Desktop y el PBIP

Visto al armar el clon import de `publico/` con Desktop de agosto de 2026.

- **Desktop abre la caché del modelo antes que el TMDL**, y le aplica la definición encima. Con
  `.pbi/cache.abf` de un modelo `es-ES` y un TMDL `en-US`, el PBIP no abre:
  `PFE_TM_DDL_MODIFIED_CULTURE_OR_COLLATION_AFTER_CHILDREN_CREATION`, porque la cultura no se
  cambia en un modelo que ya tiene objetos. Sin la caché, arma el modelo desde el TMDL.
- **"Número decimal fijo" es `decimal` en TMDL y "número decimal" es `double`.** Al refrescar,
  Desktop ajusta el tipo de la columna al que devuelve Power Query: un parquet `double` sobre una
  columna `decimal` la deja en `double`. Con el cambio recalcula el resumen automático, y una
  columna en `none` pasa a `sum`; con `SummarizationSetBy = User` se queda en `none`.
- **La fecha/hora automática no hace nada en Direct Lake y en import sí**: con
  `__PBI_TimeIntelligenceEnabled = 1`, el refresh crea una tabla de fechas oculta y una relación
  por cada columna de fecha. Una columna `date` del parquet queda en *Long Date* con
  `UnderlyingDateTimeDataType = Date`.
- **Un PBIP que ya refleja todo eso sobrevive abrir, refrescar y guardar sin cambiar un byte** del
  TMDL ni del PBIR, fuera de `cache.abf` y `localSettings.json`.
