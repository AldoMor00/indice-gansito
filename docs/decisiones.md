# Decisiones

Qué se eligió, por qué, y qué cuesta. Cada decisión lleva tres bloques: **Decisión** dice qué se
hace; **Motivo**, por qué y qué se descartó; **Costo**, lo que se acepta a cambio, cuando lo hay.
Leyendo sólo los bloques de Decisión se tiene la vista completa del proyecto. Donde algo no se
haría así en producción, se dice.

Están numeradas en el orden en que se tomaron y agrupadas por tema. Los números se citan en
código, commits y PRs, así que no cambian. Cuando una decisión posterior ajusta a una anterior,
lo dice en cursiva bajo el título.

Aquí no hay cifras salvo cuando la cifra es la decisión. Lo medido está en
[`mediciones.md`](mediciones.md), lo que la plataforma hace en [`plataforma.md`](plataforma.md),
y lo que hay que configurar a mano en [`entorno.md`](entorno.md).

| tema | decisiones |
|---|---|
| [Alcance y ambientes](#alcance-y-ambientes) | 1, 4, 5, 6, 11, 18 |
| [Zona raw e ingesta](#zona-raw-e-ingesta) | 2, 9, 34, 36, 37, 43 |
| [Bronze y notebooks](#bronze-y-notebooks) | 3, 7, 8 |
| [Silver: identidad y calidad](#silver-identidad-y-calidad) | 12, 13, 15, 16, 17, 38, 39 |
| [Escritura y layout de tablas](#escritura-y-layout-de-tablas) | 14, 32, 33 |
| [Pipelines y operación](#pipelines-y-operación) | 10, 31, 41 |
| [Gold y el índice](#gold-y-el-índice) | 19, 42, 20, 27, 35, 40, 21, 24 |
| [Modelo y reporte](#modelo-y-reporte) | 22, 23, 25, 26, 28, 29, 30 |

---

## Alcance y ambientes

### 1. La zona raw vive fuera de Fabric

**Decisión.** El histórico se guarda en el repositorio `indice-gansito-datos`, no en OneLake.
Fabric lee de ahí.

**Motivo.** La capacidad es una trial y va a desaparecer con sus datos. Con el raw afuera, Fabric
queda desechable: se borra el workspace y se reconstruye sin perder historia.

**Costo.** Git no es un almacén de datos. En producción el raw viviría en ADLS.

### 4. Prod no está conectado a git

**Decisión.** Sólo `ws-gansito-dev` tiene git integration. A prod se le despliega desde `main`
con `fabric-cicd` y credencial federada OIDC, sin secretos guardados. El despliegue no borra
huérfanos y excluye los `Lakehouse`. Rollback es revertir el commit.

**Motivo.** La rama no representa el estado deseado completo de prod: `unpublish_all_orphan_items`
borraría cualquier `Report` o `SemanticModel` creado a mano ahí. Los lakehouses de prod se crean a
mano y son los dueños de los datos; el CI no tiene por qué administrarlos. Que se lleve un
lakehouse no es el riesgo: está fuera del alcance y detrás del feature flag
`enable_lakehouse_unpublish`.

**Costo.** Prod acumula huérfanos: un item borrado en dev sigue vivo en prod hasta que alguien lo
borre a mano. Se prefiere limpiar a mano a un borrado destructivo automático.

### 5. Dos ambientes, sin test, y dev es un clon de prod

**Decisión.** Dev arranca como `SHALLOW CLONE` de los lakehouses de prod y re-corre sólo la capa
que desarrolla. Mismas tablas, mismos nombres y mismas rutas en los dos workspaces; el código no
parametriza nada.

**Motivo.** El clon copia el metadato, no los datos, y aun así es escribible y aislado: lo que dev
escribe crea archivos propios y no toca el origen (`plataforma.md`). Un shortcut no daría eso,
porque es de sólo lectura. Un tercer ambiente no tendría qué probar que dev no pruebe.

**Costo.** El clon es una foto: se refresca re-clonando (#33), y `OPTIMIZE` seguido de `VACUUM`
en prod lo rompe. Es la letra chica del clon zero-copy de Snowflake y de Databricks.

### 6. Dos modelos semánticos, y gold sale a git para el público

**Decisión.** El modelo de Fabric es Direct Lake sobre `lh_gold`. El público es un PBIX en modo
import, en My Workspace de una cuenta sin capacidad, que lee las ocho tablas de gold como parquet
desde `indice-gansito-datos/publico` por URL anónima. Reusa el TMDL y el PBIR tal cual; sólo
cambian las particiones. Los parquets los escribe `nb_50_export`, a mano. Bronze y silver no van
a git.

**Motivo.** Direct Lake exige capacidad y la trial expira: los items de Fabric se borran a los
siete días y los de Power BI no. Se necesita capacidad para *producir* gold, no para *mostrarlo*.
Gold ya es el agregado, así que exportarlo verbatim copia las medidas y las páginas sin reescribir
una línea. Parquet y no CSV porque pesa la quinta parte y Power Query lo lee directo. Gold va a git
porque algo aguas abajo lo necesita como origen; bronze ya es el raw sin castear (#8) y silver es
derivado determinista de raw más código. Guardar capas derivadas debilitaría lo que el repo afirma:
que con el raw y el código se reconstruye todo.

**Costo.** El DAX vive duplicado, como copia y no como reescritura. Cada reexport suma alrededor de
un megabyte al repo de datos.

### 11. Los dos workspaces van en la misma versión de runtime

**Decisión.** Dev y prod corren el mismo runtime (hoy 2.0). Se suben los dos o ninguno.

**Motivo.** Las tablas nacen en un protocolo de Delta con deletion vectors que un runtime más viejo
no lee, y el clon de #5 pone el bronze de prod en dev. Un workspace rezagado dejaría de poder
clonar al otro. El switch del runtime es reversible; el protocolo de una tabla, no.

**Costo.** Vive fuera de git (`entorno.md`).

### 18. Ni variable library ni user data functions

**Decisión.** No se usan, aunque `fabric-cicd` soporte las dos.

**Motivo.** La variable library parametriza lo que cambia entre ambientes, y aquí no cambia nada:
mismos nombres y rutas, resueltos en vivo (#3, #5). Encima, el value set activo es setting del
workspace y no viaja en el item. La user data function centraliza lógica sin sesión de Spark y por
REST, y los helpers de `nb_00_config` toman y devuelven DataFrames; sólo `manifiesto_de` cruzaría.
Paga con write-back o consumidores externos, y no hay ninguno.

---

## Zona raw e ingesta

### 2. Se filtra en la puerta

**Decisión.** De cada CSV de Profeco se persisten dos cortes: las filas del catálogo objetivo
(`objetivo.yml`) y las tuplas distintas de tienda, estas del archivo **completo**. El archivo
íntegro no se guarda. El manifiesto guarda el `sha256` y la URL de origen de cada uno.

**Motivo.** Cada CSV pesa más de cien megabytes y git no es para eso (#1). Las tiendas salen del
archivo completo para que `dim_tienda` no quede sesgada a las que venden pastelillos.

**Costo.** Rompe la inmutabilidad del raw y es la concesión más grande del proyecto. Se mitiga con
el manifiesto: cualquier corte se rehace desde la fuente de forma verificable. En producción no se
haría.

### 9. Cada fuente ingesta por su lado

**Decisión.** Un script y un manifiesto por fuente; en el repo de datos cada una cuelga de su
directorio. Lo único compartido es `descarga()` y `leer_manifiesto()`, en `scripts/fuente.py`.
Profeco se llavea por `(quincena, intento)`; CONASAMI e INEGI por `(archivo, version)`, y su
versión la decide el `sha256`. CONASAMI e INEGI se guardan tal cual, sin convertir. El `sha256`
es el de los bytes que sirvió el host, calculado al descargar.

**Motivo.** No se parecen. Profeco entrega lotes grandes e inmutables por quincena, que se cortan
en la puerta (#2). CONASAMI e INEGI entregan un archivo chico que se reescribe en su lugar: no hay
período ni corte, y lo que decide si hay algo que hacer es el hash. Se bajan siempre y se escriben
sólo si cambiaron, que es justo lo que Profeco no puede hacer. Sin convertir porque son decenas de
kilobytes, no hay nada que cortar, y un parquet no es reproducible byte a byte. Con el hash de la
descarga, la comparación siempre es descarga contra descarga y git nunca entra en ese lazo.

### 34. Profeco se lee del portal, por bundle anual

**Decisión.** La ingesta baja de `datos.profeco.gob.mx`, un bundle por año, y el canal viejo
(`repodatos`) se descarta. No se re-ingesta nada: el `url_origen` de las líneas viejas se queda
apuntando al host caído. La corrida se decide con el CSV de metadatos antes de bajar el bundle.
El token de cada año se lee del listado en cada corrida. El script no descomprime rar: `--local`
procesa CSV extraídos a mano. El manifiesto guarda el `crc32` de cada quincena; al abrir el
bundle de un año se cotejan sus quincenas ya procesadas, y lo reescrito entra como `intento`
nuevo. `--rehacer` fuerza sin consultar sellos.

**Motivo.** `repodatos` sirvió hasta `2025-11_q2` y desde entonces contesta 503; Profeco publica
mensual en su portal. Las quincenas viejas tienen el mismo `sha256` dentro de los bundles, así que
la serie no se parte y el manifiesto sigue siendo válido: es bitácora de lo que pasó, no catálogo
de lo alcanzable. El bundle se baja entero o nada y no dice si cambió (`fuentes.md`), de ahí la
sonda. El token no es derivable, de ahí leer el listado: 2027 entra solo el día que aparezca su
renglón, y si cambian la plantilla la corrida truena. El CRC32 del directorio central detecta una
quincena reescrita sin descomprimir, que el tamaño solo no ve. Sólo se coteja el año que hubo que
bajar de todos modos: abrir uno nada más por revisar tiraría el ahorro de la sonda.

**Costo.** 2025 viene en rar y se procesa a mano. Un año cerrado no se vuelve a revisar solo.

### 36. El corte de precios declara sus columnas, y el catálogo del portal no se ingesta

**Decisión.** `corte_precios` declara sus 15 columnas y truena si falta alguna, igual que
`corte_tiendas`. El recorte va en la ingesta, no en bronze. La API JSON del portal de Profeco no
se ingesta.

**Motivo.** Junio de 2026 llegó con tres columnas de más (las llaves internas de Profeco) y bronze
tumbó la corrida al escribir con `mergeSchema` apagado (#8): la alarma funcionó; faltaba la
respuesta. En la ingesta porque bronze no filtra (regla #2) y la zona raw ya es donde el proyecto
concede recortes (#2). Los parquets de junio se rehicieron sin `intento` nuevo: el `sha256` del
manifiesto es del CSV de origen, que no cambió; fue defecto de extracción nuestro, y git conserva
lo que produjo. La API no mete las llaves en la historia: los archivos masivos siguen sin
traerlas, unir por texto ya falla en uno de los nueve SKUs, el padrón de establecimientos no
sustituye a `dim_tienda` y el endpoint de precios es del día (`fuentes.md`). Queda medido por si
algún día Profeco publica las llaves en los archivos: ahí cambiaría la identidad del modelo.

### 37. La ingesta comprueba la codificación, no la supone

**Decisión.** Si el archivo entero no decodifica como utf-8 (por trozos, sin cargarlo), se lee
como cp1252 y se dice en la corrida; si tampoco, truena. El BOM no se mira. La codificación
detectada se guarda en el manifiesto.

**Motivo.** Mayo de 2026 llegó en cp1252 y `utf8-lossy` cambió cada byte inválido por `U+FFFD` sin
fallar: miles de celdas corruptas entraron al repo y lo cazó, tres capas después, la compuerta de
silver que cuenta presentaciones. utf-8 va primero porque es autovalidante: si pasa, es. El
respaldo es uno solo y nombrado, y no `latin-1`, que acepta los 256 bytes y por eso nunca detecta
que está equivocado. Al respaldo sólo se llega cuando el archivo ya demostró no ser utf-8, así que
no hay cascada silenciosa. El BOM es opcional y el estándar lo desaconseja: "sin BOM ⇒ cp1252"
convertiría el primer utf-8 generado fuera de Windows en mojibake. Al manifiesto para que quede
rastro durable de que esa quincena se leyó distinto.

**Costo.** Un archivo que no sea utf-8 ni cp1252 pero que cp1252 acepte (otra codificación de un
byte, o utf-16) pasaría con letras equivocadas. Es estrecho, no ha ocurrido, y lo cazaría una
compuerta de silver, como esta vez.

### 43. Un hueco permanente en Profeco detiene gold, y se acepta

**Decisión.** La ingesta se salta la quincena que no viene en el bundle. Gold truena si el
calendario de silver tiene un hueco (`exige_sin_huecos`). No se construye salida para un hueco
permanente.

**Motivo.** Saltarse en la ingesta evita que un hueco atore al cron antes de llegar a lo
publicado. Gold truena porque sin la quincena anterior el eslabón no existe y el índice quedaría
plano sin avisar. La fuente ha cambiado de forma pero nunca ha dejado de publicar una quincena. El
fallo es ruidoso (los ordinales del hueco van en el mensaje y *Schedule failures* avisa) y no
corrompe: gold truena antes de escribir. Si el hueco es temporal se cura solo.

**Costo.** Si un día es permanente, raw, bronze y silver avanzan y gold se queda congelado en la
última quincena antes del hueco, tronando cada martes, hasta que se decida con el caso a la vista.

---

## Bronze y notebooks

### 3. Los notebooks no usan lakehouse por defecto

**Decisión.** Ningún notebook lleva lakehouse enlazado; `nb_00_config` truena si lo hay. Leen su
workspace en tiempo de ejecución y arman la ruta `abfss` por nombre. Lo que sí hay que reasignar
al desplegar (refresh, Direct Lake, schedules) vive en `fabric/parameter.yml`.

**Motivo.** El enlace del UI guarda el GUID del lakehouse: al desplegar a otro workspace sigue
apuntando al origen y el notebook corre en verde sobre los datos equivocados.

### 7. Nada de wheels: `%run` y pruebas en notebook

**Decisión.** Los helpers se comparten con `%run nb_00_config`. Las pruebas de código van en
`nb_90_pruebas`, que se corre a mano; las de datos son las compuertas (#16), dentro del notebook
que escribe la tabla. La observabilidad no lleva notebook propio: sale por `apunta()` al resumen
de la corrida. El Python de GitHub Actions sí lleva `pytest` y `ruff`. Cada fallo tiene su trato:
código roto no se commitea; carga incompleta truena el job (bronze reconcilia su conteo contra el
manifiesto); dato malo truena igual (#15).

**Motivo.** Publicar una wheel a un Environment toma minutos y mata la iteración. El esfuerzo va
sobre datos y no sobre código: la lógica pura es poca y sus errores salen a la primera corrida;
los incidentes vienen de la fuente. Las compuertas son precondiciones de la escritura y por eso
viven junto a ella. Un notebook de observabilidad tendría que releer bronze entero para recalcular
lo que el que escribe ya tiene en memoria. El pipeline no distingue "la fuente cambió" de "el
pipeline se rompió" porque el arreglo es el mismo y lo hace la misma persona.

**Costo.** `nb_90_pruebas` se queda chico y no corre en ningún pipeline.

### 8. Bronze lee por HTTPS, no por shortcut

**Decisión.** El notebook baja los parquets de `raw.githubusercontent.com` con `requests`; el
manifiesto es el índice y las rutas se derivan de él. Aterriza en `Tables/dbo` como Delta y
`Files` se queda vacío. Escribe con `mergeSchema` apagado.

**Motivo.** Los shortcuts de OneLake no hablan con GitHub, y el repo no lista directorio. Una copia
en `Files` sería la tercera (el parquet en git y la tabla, que no castea, ya son iguales) y no
podría ser fuente de verdad porque vive en la capacidad desechable (#1). `mergeSchema` apagado
porque el esquema de la fuente está medido: una columna nueva es alarma de lote, no algo que se
absorba. #36 lo confirmó.

**Costo.** En producción sería un shortcut a ADLS y `Files` sería ventana, no copia. Lo que falta
sin él no es "cero copia" (bronze materializa igual) sino que un archivo nuevo aparezca sin correr
nada.

---

## Silver: identidad y calidad

### 12. El grano de silver es la quincena, y el precio es un promedio

**Decisión.** Silver lee de bronze el `intento` máximo de cada quincena y agrega a
tienda-SKU-quincena. La columna es `precio_promedio`, con `observaciones`, `precio_min` y
`precio_max` de la misma agregación. Los grupos con dos precios distintos el mismo día no se
tratan aparte.

**Motivo.** Profeco visita la misma tienda hasta cinco veces por quincena y `fecha_registro` no
trae hora, así que las visitas del mismo día no se pueden ordenar. Promediar conserva las
observaciones en vez de escoger una sin criterio; cuál se tome es inmaterial para el índice
(`mediciones.md`). Las tres columnas dejan ver si el número se observó de verdad
(`precio_min = precio_max`) sin una bandera booleana derivable que habría que mantener al día.
Las observaciones exactas viven en bronze.

### 13. La identidad sale de la fuente

*Ajustada por #38 (la coordenada) y #39 (la grafía).*

**Decisión.** Una tienda es `(nombre_comercial, direccion)` y un SKU es `(presentacion, marca)`.
De `presentacion` se parsean piezas y gramaje, como atributos y no como clave. La canasta son
nueve SKUs: las Barritas Fresa y Piña se excluyen de toda la serie, en silver y no en
`objetivo.yml`. Que todos parseen es compuerta.

**Motivo.** Son las únicas claves mínimas medidas en la fuente (`fuentes.md`); `producto` y
`categoria` son constantes, y la coordenada no identifica aunque no se mueva. `(piezas, gramos)`
colisiona en los dos Panqués y en las dos Barritas. Las Barritas salen porque Profeco las
reclasificó a Galletas Dulces a mitad de la serie; la exclusión no cabe en `objetivo.yml` porque
el filtro de ingesta es por `producto` y ahí comparten valor con el resto.

**Costo.** La identidad de una tienda es exactamente su grafía. Lo que eso cuesta lo cubre #39.

### 15. Silver es fail fast: pasa o truena, sin cuarentena ni bandera

**Decisión.** Lo que no cumple una compuerta detiene la corrida antes de escribir. No hay tabla de
cuarentena, ni contador de inválidas, ni bandera de válido. Tampoco hay tier de aviso: la prueba
es "¿publicarías el dato con esto sin resolver?"; si no, es un error; si sí, es un atributo del
dato o una métrica del lote, y ya tienen dónde vivir.

**Motivo.** Las tres alternativas se probaron y comparten el defecto: dejan aterrizar dato que
todavía no se confía, y desde ahí cada conteo depende de que alguien se acuerde del `WHERE`. Lo
único que publica este proyecto son números. `replaceWhere` sólo toca las quincenas pendientes,
así que la corrida que truena deja lo anterior intacto: viejo pero cuadrado contra fresco pero
descuadrado. Una bandera gana con varios consumidores de tolerancias distintas o con un SLA; aquí
hay un consumidor y un operador. Y como nada se escribió, la quincena sigue pendiente: se arregla
el notebook, se re-corre y entra sola.

**Costo.** Se pierde el registro persistente de qué falló. Se compensa: la corrida se detiene en
un notebook, sobre un lote conocido, con la columna nombrada en el error.

### 16. Dónde vive cada compuerta, y por qué ninguna se repite

**Decisión.** `nb_00_config` enciende ANSI, así que `cast` truena y `try_cast` da nulo: `cast` es
"esto tiene que pasar", `try_cast` es "esto puede faltar" y lleva comentario. El tipado no tiene
compuerta propia. Lo demás se reparte: entrada, sobre el lote crudo (obligatoria vacía, un
atributo por clave, formato del que cuelga un regex); salida, sobre el DataFrame armado (llave sin
colisión); constraint CHECK de Delta, sobre la fila (`precio_min > 0`, `gramos > 0`,
`piezas > 0`). Las constraints las aplica el notebook, idempotentes. La corrida va en tres
tiempos, armar, validar, escribir, con las escrituras al final. No hay `NOT NULL` sobre ids.

**Motivo.** Fabric trae ANSI apagado, contra el default de Spark 4 (`plataforma.md`). Cada
compuerta va donde alcanza a ver: la de entrada es sobre la fuente y su mensaje es lo que se
repara; la de salida sólo existe después de transformar; la constraint es lo único que Delta sabe
expresar. Spark es perezoso, así que hasta el `.write` los DataFrames son la etapa de staging:
Write-Audit-Publish sin la tabla intermedia ni el swap. Una constraint puesta con DDL a mano
desaparece al recrear la tabla, y una protección que crees tener y no tienes es peor que ninguna.
`NOT NULL` sobre un id es teatro: `xxhash64` nunca da nulo; el riesgo real es una llave válida y
**equivocada** cuando la clave natural viene vacía, y eso lo ataja la entrada.

### 17. La zona del salario mínimo son 6 identidades de 7 literales, y su vigencia se cierra global

**Decisión.** `resto del pais` y `general` son la misma zona: la identidad va sobre el nombre
normalizado y el literal queda de atributo. `unica` no se fusiona con ellas. El cierre de
vigencias es global, no por zona. La vigencia abierta se cierra con `9999-12-31`, no con nulo.
Una compuerta truena si una zona reaparece después de cerrarse.

**Motivo.** El renombre está medido (`fuentes.md`). `unica` cubría el mismo país, pero al crearse
la ZLFN se le recortó territorio: cambio real, no de nombre. CONASAMI reexpide el tabulador
completo, así que una vigencia termina cuando entra el siguiente; partido por zona, las históricas
quedarían abiertas para siempre y una quincena de 2024 casaría con las seis. El centinela deja que
`es_vigente` sea una comparación y que el `BETWEEN` de gold no arrastre un `OR IS NULL`.

**Costo.** El punto ciego es una zona que sale de un tabulador y vuelve en el siguiente. No ha
pasado, y la compuerta lo cazaría.

### 38. La coordenada es atributo, no obligación

**Decisión.** `latitud` y `longitud` salen de la compuerta de completitud. Siguen en el `cast` a
`decimal(9,6)` y en `exige_uno_por_clave`.

**Motivo.** Desde `2026-04_q2` Profeco da de alta tiendas sin coordenada, y la cadena se detenía
por tiendas que ni siquiera venden del catálogo. La coordenada no identifica (Profeco geocodifica
el mercado, no el local) y sólo pinta el mapa: una tienda sin ella no aporta punto y sí aporta
precio. Detener la corrida por eso confunde presentación con medición. La distinción que queda
escrita: **falta un dato que necesito** contra **falta un dato que uso si está**.

### 39. El texto de tienda se normaliza en silver, y la identidad depende de ello

**Decisión.** Antes de cualquier compuerta y de `clave()`, las columnas de tienda pasan por tres
mapas: `ACENTOS` pliega las vocales acentuadas a ASCII y repara `ð` y `´`; `PALABRAS` lleva cada
palabra con `?` a su forma limpia, tomada de la propia fuente; `TYPOS` unifica los cinco pares que
la fuente se contradice sola, por columna. `PALABRAS` va congelado en el notebook, no derivado en
cada corrida. Cierra la compuerta `exige_caracteres`: fuera del ASCII imprimible, de `ñÑ°¡ºª` y
de lo resuelto, nada pasa, y `?` está prohibido. Va en silver, no en la ingesta. Las columnas de
producto no se normalizan.

**Motivo.** `id_tienda` es el hash de la grafía, y Profeco escribe la misma tienda con acento y
sin él, y desde 2026 también con el acento perdido como `?`: había miles de identidades de más,
todas del lote nuevo (`mediciones.md`). Se pliega y no se acentúa porque sólo esa dirección es
determinista, y porque `CLAVE_ESTADO` espera los estados sin acento, como el TopoJSON del mapa. En
silver porque bronze guarda lo que publicó la fuente y el `sha256` describe ese CSV (#9): resolver
identidad es de silver. Congelado porque un mapa que se recalcula puede resolver distinto al llegar
una quincena y mover `id_tienda` en silencio, que es el fallo que esta decisión cierra. Los
desempates se hicieron con la cadena gemela completa y no por frecuencia, que se habría equivocado
en cuatro. `TYPOS` va por columna porque `Central de Abasto` es también un `giro`, y ahí es la
llave de `CANAL` en gold. La premisa de #13 sobrevive: normalizados, ningún atributo cambia bajo
la clave, así que `dim_tienda` sigue sin necesitar SCD2.

**Costo.** Una palabra nueva con `?` detiene la corrida hasta que se agregue. Tres riesgos quedan
escritos: el mapa es por palabra y generaliza (un `Quintana R?o` quedaría `Quintana Rio`); seis
entradas copian un typo de la fuente, porque el trabajo es reparar codificación y no corregirle
la ortografía a Profeco; y `Maron` → `Marin` actúa sobre texto limpio y afectaría a cualquier
otra tienda con `Marón`. Los mapas viven en `nb_20` y `nb_90` no los prueba. Cambiar la llave
obligó a dropear `dim_tienda` y recalcular con `quincenas_pedidas = "todas"`.

---

## Escritura y layout de tablas

### 14. Un patrón de escritura por tipo de tabla, y nunca `overwrite`

**Decisión.**

| tabla | patrón |
|---|---|
| bronze | `append`, `mergeSchema` apagado |
| dimensión | `MERGE` por clave, con condición de cambio `NOT (d.col <=> n.col)` |
| dimensión SCD2 | `MERGE`, cerrando vigencias con `LEAD` |
| hecho | `replaceWhere` sobre las quincenas recalculadas |

`overwrite` de la tabla completa no se usa en ninguna.

**Motivo.** El patrón lo decide la tabla, no el tamaño: se elige el que se usaría en producción
aunque a esta escala sea overkill, porque el volumen de la muestra es un accidente del dataset
público. Una dimensión es acumulativa: la tienda que salió del panel no deja de existir y los
hechos viejos la siguen apuntando. CONASAMI reexpide la historia completa, así que la vigencia se
deriva del lote y el mismo MERGE la cierra. La quincena está completa o no está; un MERGE leería
la tabla entera buscando filas que por construcción no existen. Reescribirlo todo esconde justo
lo que hay que ver, qué cambió en esta corrida, y deja el historial sin nada que comparar. Sin la
condición de cambio, la corrida sin novedades reescribe archivos y el log deja de distinguir "no
pasó nada" de "se recalculó todo"; con ella, un MERGE que no cambia nada no commitea versión.

**Costo.** Las métricas de un MERGE se leen comparando versiones, no mirando la última entrada
del log (`plataforma.md`).

### 32. El hecho se clusteriza, no se particiona

**Decisión.** `hechos_precios` (silver y gold) y `hechos_relativos` van con liquid clustering por
`(_quincena, id_producto)` y sin `partitionBy`. El layout se declara con un `ALTER` idempotente
(`exige_clustering`), no en el writer. El `replaceWhere` de #14 no cambia.

**Motivo.** Fabric publica que particionar es para aislar escritores concurrentes y pide al menos
un gigabyte por partición. Los dos fallan aquí: hay un escritor por diseño, y a volumen de
producción una quincena de Profeco queda muy por debajo del tamaño de archivo que el runtime
calcula como bueno, con 24 particiones nuevas al año para siempre. La partición venía del runtime
1.3, donde el clustering reescribía la tabla entera; el modo incremental llegó con 2.0. La doc de
Direct Lake recomienda lo opuesto, pero pide row groups de millones de filas y una quincena no
llega. Las dos columnas son las que aparecen en todo predicado; `id_tienda` queda fuera por
cardinalidad. El `ALTER` porque `clusterBy` del writer es un no-op silencioso sobre tabla por ruta
(`plataforma.md`). El write amplification que se temía no ocurre: las deletion vectors marcan las
filas viejas y las nuevas se escriben aparte.

**Costo.** A este volumen `OPTIMIZE` se salta la tabla, así que el clustering está declarado y
nunca aplicado: lo que ordena las filas hoy es el V-Order. El layout está elegido para el caso
real, y sólo el caso real lo va a ejercer.

### 33. El mantenimiento va programado y por notebook, y el V-Order no entra por ahí

**Decisión.** `pl_mantenimiento` corre semanal y en este orden: `OPTIMIZE` → `VACUUM` (`RETAIN 168
HOURS`, escrito) → refresh de `sm_gansito` → re-clon del bronze de dev. Va en `nb_60`, no en la
*Lakehouse maintenance activity*. Sin auto compaction. El V-Order entra por el perfil
`readHeavyForPBI` en la escritura de `nb_30`; el `VORDER` del mantenimiento sólo lo conserva. Todo
notebook que escribe declara su perfil de recursos. `nb_91` resuelve origen y destino por nombre,
depende del refresh con `Succeeded`, y su `rm` sólo puede caer en dev. En prod corre con la cuenta
del despliegue.

**Motivo.** Aparte de la ingesta porque las cadencias no coinciden. El orden: un modelo Direct
Lake frameado referencia una versión concreta, así que un `VACUUM` antes del refresh tumba las
queries del reporte; y `OPTIMIZE` seguido de `VACUUM` rompe el clon (#5), así que el re-clon va
después de los dos. La activity del pipeline no soporta lakehouses con esquemas, y los tres lo
son; el notebook además deja usar `DRY RUN` y la retención por sesión. Los siete días escritos
dejan la intención en el código; bajar exigiría apagar `retentionDurationCheck` sin razón. Auto
compaction está pensado para streaming: su umbral son decenas de archivos chicos y una corrida
quincenal escribe uno; además commitearía versiones que no vienen de ninguna corrida (#14).
`OPTIMIZE … VORDER` no V-Ordena hacia atrás lo que decide no reescribir (`plataforma.md`), y la
sesión de mantenimiento corre en `writeHeavy`, que lo trae apagado y le gana a la propiedad de
tabla. El perfil va explícito en cada notebook porque con High concurrency la sesión se comparte y
el perfil del anterior seguiría puesto. `Succeeded` y no `Completed` porque el pipeline sale verde
o rojo por su última actividad, y un clon exitoso taparía un refresh tronado.

**Costo.** Un refresh fallido deja una semana sin re-clon, que se corre a mano. Lo que le da
trabajo real al mantenimiento es `hechos_ic_indice`, que el MERGE deja en varios archivos chicos,
y las deletion vectors de cada recálculo.

---

## Pipelines y operación

### 10. `pl_bronze`: encadenado, con sesión compartida y un nodo de unión

**Decisión.** Las actividades de bronze van encadenadas con *on completion* y comparten sesión con
el `sessionTag` `bronze`. El reintento se queda en la primera, que es la que pide sesión. Cierra
`fuentes_ok`, un `Wait` de un segundo que depende de todas con *on success*.

**Motivo.** Sueltas, dispararon en el mismo segundo y una se fue con 430: la trial no da para dos
sesiones de Spark, y dev y prod comparten capacidad. *On completion* encadena por capacidad, no
por dependencia, así que el fallo de una no impide que la otra cargue. Compartir sesión no acelera
nada medible, pero la segunda ya no pide sesión y el 430 entre ellas deja de ser posible. *On
completion* sola miente: una actividad que falla con sólo camino de completion se da por manejada
y el pipeline reporta éxito. Con dependencias múltiples evaluadas con AND, el `Wait` deja el
camino sin tomar y el pipeline truena, como manda #7.

**Costo.** El tag no basta solo: High concurrency se prende en cada workspace, fuera de git
(`entorno.md`).

### 31. La observabilidad de las corridas es el monitoring hub

**Decisión.** No hay `lh_ops` ni tabla de corridas. El estado de cada corrida va en el exit value
(`pendientes=0` incluido) y el hub lo guarda. El aviso de fallo es *Schedule failures* del hub,
configurado en cada pipeline programado (`entorno.md`).

**Motivo.** La corrida sin pendientes no escribe y `DESCRIBE HISTORY` no la ve, pero el hub sí. Lo
que la tabla agregaba era retención y consulta como serie, y la retención del hub (30 días de
actividades, 60 de historial de notebook) supera la vida de la trial: nunca contendría algo que
el hub no tuviera. Bronze trae `_ingestado_utc` y `_corrida` por fila, así que qué aterrizó y
cuándo se reconstruye del dato mismo. Para años de historial la respuesta de la plataforma es
workspace monitoring o la Job API, no una tabla Delta a mano. Además `activityId` es de la
sesión y no de la ejecución, así que no serviría de llave (`plataforma.md`).

### 41. `pl_master` encadena las capas con una espera fija entre pipelines

**Decisión.** `pl_master` invoca `pl_bronze` → `pl_silver` → `pl_gold` con *on success*, con
*Invoke pipeline (Legacy)* y un `Wait` de 120 s entre invokes. Los tres pipelines siguen
existiendo por separado. Sin `sessionTag` común entre pipelines.

**Motivo.** Una capa roja detiene las siguientes (#7) y una sola se puede rehacer. El invoke nuevo
exige una conexión fuera de git; el Legacy sale del commit con `logicalId`. El `Wait` fijo
contradice lo medido dentro de `pl_bronze`, y la diferencia es la causa: entre pipelines lo que
ocupa la capacidad es la sesión del anterior, que tarda hasta dos minutos en cerrar después de que
su pipeline termina (`plataforma.md`), y eso sí se mide. Apoyarse en el reintento funciona, pero
gasta en cada corrida el único reintento de la primera actividad. La espera va en `pl_master`
porque un pipeline corrido solo no la necesita. Un tag común no cabe: bronze y silver suman seis
notebooks contra un límite de cinco por sesión, y subirlo exige un Environment (regla #5).

**Costo.** Los hijos no aparecen en su propio historial; se leen desde la corrida del padre.

---

## Gold y el índice

### 19. El cambio de precio se publica con Jevons encadenado, no con un promedio

*Ajustada por #42 (la guarda).*

**Decisión.** Cada quincena se parea contra la inmediata anterior sobre las tiendas presentes en
las dos, y los eslabones se encadenan. La media del eslabón es Jevons. `hechos_relativos` se
materializa al grano de tienda y el índice es una sola medida DAX. Si el corte no junta el pareo
mínimo, la medida devuelve `BLANK()`; gold no se filtra. El umbral arranca en 30 tiendas y se
expone como parámetro.

**Motivo.** El panel rota, así que comparar el promedio de la primera quincena contra el de la
última mezcla cambio de precio con cambio de muestra. El panel balanceado, que parece la corrección
obvia, retiene una fracción chica del n y sale sesgado por supervivencia; el encadenado retiene
casi todo (`mediciones.md`). Jevons y no Dutot porque vive en logaritmos, y los logaritmos son
aditivos: eso deja el índice en una medida en vez de una tabla pre-agregada. Carli sobreestima. Al
grano de tienda porque el encadenado no es aditivo entre cortes: pre-agregar eslabones por corte
congelaría los cortes para siempre. La guarda va en la medida porque el dato entra completo
(filtrarlo sería compuerta de silver en la capa equivocada) y los cortes transversales no
necesitan pareo. Es lo que decide que el corte de canal sean tres categorías y no los cinco
`giro` de la fuente.

**Costo.** Ninguna cadena alcanza el umbral, así que por cadena se publica nivel y no índice (#25).

### 42. La guarda del índice mira el conjunto de eslabones, no el más flaco

**Decisión.** Se publica si el pareo efectivo alcanza el umbral y ningún eslabón tiene menos de 10
tiendas. El pareo efectivo es la media armónica de las tiendas pareadas por eslabón. `Publica
índice` es la única guarda; la línea la evalúa sobre los eslabones de la base al punto, así que
su último punto es la tarjeta. Los eslabones flacos no se excluyen. Una serie apagada por el piso
no vuelve.

**Motivo.** La regla anterior (el umbral al eslabón más flaco) la aplicaban distinto la tarjeta y
la línea, y con 62 quincenas conveniencia lo destapó: un solo eslabón por debajo dejaba la tarjeta
en blanco mientras la línea se dibujaba entera. El ruido del encadenado crece con la suma de 1/n,
así que la media armónica es el n de una cadena uniforme igual de ruidosa: un eslabón flaco entre
muchos sanos casi no la mueve; muchos flacos, o uno muy flaco, sí. Toda cadena que la regla vieja
aceptaba sigue pasando. El piso de 10 es robustez y no muestra: con menos, una sola tienda pesa
más del 10 % del eslabón. Excluir un eslabón equivale a suponer que el precio no se movió esa
quincena, y en cortes chicos los flacos no caen al azar: en un caso medido borraba una bajada real
(`mediciones.md`). Calibrar con bootstrap por corte mide mejor, pero exige materializar márgenes
por canal y cadena en gold, y el proyecto es de ingeniería de datos.

**Costo.** La media armónica no ve la dispersión de precios dentro del corte. Queda medido.

### 20. El intervalo del índice se materializa en gold, tabulado por SKU

**Decisión.** `nb_30_gold` calcula el IC 95 % por bootstrap de tiendas, 2,000 réplicas y semilla
fija, y deja `hechos_ic_indice` al grano SKU × quincena. Se calcula al final, sobre gold ya
escrito. La medida exige un solo producto en contexto y se apaga ante cualquier filtro de tienda.

**Motivo.** DAX no puede: el error del encadenado no es función de los agregados del eslabón,
porque las mismas tiendas reaparecen eslabón tras eslabón y sus errores se telescopan. Sumar
varianzas en cuadratura da una banda mucho más ancha que la real (`mediciones.md`), y publicarla
así es peor que no publicarla. Tiendas y no celdas porque la tienda es la unidad de muestreo de
Profeco y las celdas de una misma tienda no son independientes. Semilla fija para que dos corridas
publiquen el mismo intervalo. Por SKU porque el titular es el Gansito y el intervalo de la canasta
no se le parece: cada SKU se remuestrea contra su propio padrón de tiendas. Al final porque cada
quincena nueva alarga la cadena y mueve el intervalo de todas las anteriores.

**Costo.** Es la única tabla de gold que no proyecta silver. El bootstrap corre sobre el padrón
entero, así que no hay intervalo por corte.

### 27. El índice no publica 100 en la quincena base sin observación

**Decisión.** La rama de la medida que devuelve 100 en la quincena base pide
`NOT ISBLANK([Precio promedio])`. Se arregla en la medida, no con un filtro de visual.

**Motivo.** Sin esa rama la serie arranca en blanco un eslabón tarde: en la base no hay eslabón
anterior que parear. Escrita sin condición, publicaba 100 para cualquier corte, incluso sin una
celda detrás: el grupo de `canal` nulo (los giros que no venden pastelillos) dibujaba una serie
fantasma en la leyenda. En la medida porque un filtro tapa el caso en esa página, y la medida lo
resuelve en cualquier corte que venga después.

### 35. El deflactor sale del INPC de INEGI, no de CONASAMI

*Dónde vive, en #40.*

**Decisión.** El deflactor es el indicador `910420` de INEGI (INPC general, nivel, quincenal),
ingestado con `nb_12` y `nb_22`. CONASAMI se queda como fuente del salario. El token de INEGI es
secreto del repositorio y el manifiesto guarda `{token}` en su lugar.

**Motivo.** Hasta entonces el deflactor era implícito, `smg_nominal / smg_real`, y esa serie para
en enero de 2026 mientras los precios llegan a julio; `nb_30` truena a propósito cuando la ventana
de precios se adelanta a la del deflactor. Además de alcance es grano: el de CONASAMI es mensual y
el de INEGI quincenal, y empata uno a uno con `_quincena`, que es lo que `fuentes.md` ya declaraba
correcto. El cambio está comprobado: el mensual de INEGI promedia a lo que CONASAMI traía, a una
milésima. El salario mínimo se fija una vez al año y sigue vigente, así que su serie corta no lo
deja cojo. La alternativa sin token, el CSV de datos abiertos del INPC, se quedó en julio de 2024.

**Costo.** Un token en la URL.

### 40. El deflactor viaja en la quincena, y el salario se queda con su ventana

**Decisión.** El INPC es columna de `dim_tiempo_quincena`, no una novena tabla.
`hechos_salario_mensual` pierde `inpc`. `id_mes` entra por `left join` y queda nulo donde la serie
salarial no alcanza. La compuerta dura es "quincenas sin INPC"; las quincenas sin mes van al
resumen de la corrida como `sin_mes`. `nb_22` comprueba que `FREQ` y `UNIT` sigan diciendo
quincenal y nivel.

**Motivo.** Hay exactamente una fila de INPC por fila de la dimensión, y un hecho 1:1 con su
dimensión es un join que no puede abanicar ni filtrar nada. El deflactor deja de materializarse
desde CONASAMI en ninguna capa. El mes opcional deja que la página del salario termine donde
termina su fuente y el índice no; `Gansitos por día` se va en blanco sola porque divide por una
medida vacía. Completar el mes se descartó: la tabla dejaría de ser la serie de la fuente para ser
una extendida por nosotros, y eso no se cruza para que una página llegue seis meses más lejos.
Sin deflactor el índice real no existe, por eso esa es la compuerta dura; la ventana de una fuente
se mira y no bloquea (#15). La compuerta de `FREQ` y `UNIT` existe porque casi todos los
indicadores vecinos de `910420` son variación porcentual y no nivel: si el id se moviera, ni el
`cast` ni `inpc > 0` lo verían.

**Costo.** Recrear `dim_tiempo_quincena` y `hechos_salario_mensual` en gold, porque el `upsert`
no cambia esquema.

### 21. El nombre comercial del SKU se deriva en gold, con diccionario

**Decisión.** `dim_producto` gana `nombre` (no `nombre_comercial`), por diccionario explícito de
las nueve presentaciones. La compuerta mira el universo completo de la dimensión, no sólo lo que
trae precio.

**Motivo.** Profeco declara los nueve SKUs con el mismo `producto` y la misma `categoria`; lo
único que los distingue es `presentacion`, y el reporte necesita "Gansito". Diccionario y no regex
porque dos de nueve no siguen el patrón, y porque la canasta está cerrada: una presentación nueva
es una alarma, no un caso que absorber (al revés que `CANAL`). Silver ataja el SKU nuevo pero no
el renombre: la clave es el hash de la cadena, así que nace otro `id_producto` y la dimensión, que
es acumulativa, queda con la vieja y la nueva; llegaría sin nombre y sería un blanco en el slicer.
`nombre_comercial` ya es media identidad de una tienda.

### 24. La clave del mapa se deriva en gold, con diccionario

**Decisión.** El mapa es un `shapeMap`, no Azure Maps. `dim_tienda` gana `clave_estado`, el ID del
TopoJSON, por diccionario de los 32 estados. Un estado fuera del diccionario detiene la corrida.
`estado` se queda como lo escribe Profeco.

**Motivo.** Azure Maps sólo opera en regiones de Estados Unidos y la Unión Europea; fuera de ahí
exige además un switch de administración para que el dato salga del tenant, y dibuja tiendas y no
estados. Casi todos los estados casan por nombre, pero los dos que no son los dos más grandes del
padrón. Los 32 y no los observados para que Colima y Nayarit pinten el día que aparezcan. Es el
mismo patrón que #21.

**Costo.** `shapeMap` sigue en preview. Agregar la columna obligó a reconstruir `dim_tienda` en
gold: el MERGE exige las mismas columnas, y `autoMerge` de sesión lo desaconseja la doc y metería
cambios de esquema callados. Gold es proyección de silver: se dropea y se re-corre.

---

## Modelo y reporte

### 22. El histograma sale de una tabla de bins desconectada

**Decisión.** `Rango de precio` es una tabla de constantes desconectada, con bins de un peso y el
borde en el medio peso. `Tiendas en rango` devuelve cero dentro del rango observado y `BLANK`
fuera.

**Motivo.** Power BI no trae histograma. El binning del UI crea una columna calculada, y Direct
Lake no las admite; binear en gold congela el ancho dentro de los datos; un visual de AppSource
mete una dependencia externa en una pieza que se va a enseñar. La tabla desconectada es legal y
es la forma que el modelo ya usaba para `Umbral de pareo`. El ancho lo pide Freedman-Diaconis
sobre el corte real; el borde en el medio peso porque los precios de anaquel se apilan en pesos
redondos, y así la moda cae al centro de la barra en vez de partirse entre dos. Cero adentro y
`BLANK` afuera porque sin esa rama salen decenas de barras vacías a los lados, y con `BLANK` a
secas los bins vacíos de en medio se colapsan y la forma miente.

### 23. Los cortes transversales llevan un mínimo de cinco tiendas, y va en el visual

**Decisión.** Los visuales de nivel de P2 (cadena, municipio, mapa) filtran a cinco tiendas o más,
como filtro de visual y no de gold. La tabla de municipios lleva el estado como columna aparte.

**Motivo.** P2 no compara periodos, así que no hay pareo ni guarda del índice, y los cinco giros
de la fuente valen completos. Pero ordenar por precio con la muestra entera pone el ruido arriba:
la cadena más cara resulta una farmacia con dos tiendas. En el visual por la misma razón que la
guarda del índice, el dato entra completo y quien decide es la medida en su contexto, y por eso
convive con visuales de la misma página que no lo llevan. En el mapa pesa más: el área se lee como
peso, y un estado con una sola tienda saldría del color más intenso; los que no alcanzan se quedan
sin pintar, que es lo que dicen los datos. El estado aparte porque los nombres de municipio se
repiten entre estados, y agregarlos por nombre inventa una fila.

### 25. Por cadena se publica nivel, no índice

**Decisión.** P3 no lleva cambio encadenado por cadena. Publica nivel: precio al inicio contra
precio al cierre, con cinco tiendas en las dos puntas. La página dice que las dos puntas no son
las mismas tiendas.

**Motivo.** Con la guarda original ninguna cadena alcanzaba el umbral de pareo, y con la de #42
lo alcanzan dos de casi cuarenta (`mediciones.md`): el visual habría salido en blanco casi
siempre. Es el mismo resultado que obligó a colapsar `giro` a tres canales: el umbral decide la
granularidad publicable, no el gusto. El nivel no encadena nada y no le debe nada al
pareo; ahí aplica la guarda de #23, en las dos puntas y no en su promedio, porque con una sola
pasan cadenas que en la otra no existen.

**Costo.** El dumbbell mide nivel de cadena y no cambio pareado. Se dice en la página, no en una
nota al pie.

### 26. El dumbbell se dibuja con error bars, no con un visual de AppSource

**Decisión.** Una barra clusterizada con error bars: el valor es `Precio al inicio`, las cotas son
inicio y cierre con `isRelative: false`, los caps van encendidos y la barra apagada con
`fillTransparency` al 100. Eje fijo de $12 a $24, ordenado por `Precio al inicio`.

**Motivo.** Power BI no trae dumbbell. Un visual de AppSource mete una dependencia de tenant que
la copia import (#6) tendría que renderizar también; un scatter con symmetry shading es nativo y
analíticamente más fuerte, pero se lee frío con doce etiquetas; barras pareadas pierden la línea
que hace legible la brecha. Las error bars ya estaban probadas en la banda de P1. La barra se
apaga por transparencia y no pintándola del color del lienzo, para no depender del tema. El eje
fijo porque desde cero las mancuernas se apelmazan; el orden por inicio es lo que deja ver que los
que arrancaron baratos son los que más subieron.

### 28. El miembro en blanco se filtra en el slicer, no se promete integridad

**Decisión.** Cada slicer lleva un filtro de visual "is not blank". Las relaciones no se marcan con
*Assume referential integrity*.

**Motivo.** El blanco no es dato sucio: es el *unknown member* que el motor agrega del lado "uno"
de cada relación al expandir con `LEFT OUTER JOIN`, y en Direct Lake se queda porque la integridad
no se valida contra los Delta. Asumir integridad cambia el join a `INNER` y borra el miembro de
todo el modelo, pero es una promesa sobre los datos: el día que una llave de hecho no tenga su
fila de dimensión, esas filas desaparecen calladas en vez de acumularse en el blanco. Silver está
construida al revés (#15), y el índice publica en blanco antes que publicar de más.

### 29. El poder adquisitivo se publica en dos escalas, y ninguna se convierte en la otra

**Decisión.** P4 publica el nivel (`Gansitos al inicio` y `Gansitos al cierre`), las dos series
base 100 (`Índice encadenado` e `Índice salario mínimo`) y los dos cambios que explican la brecha.
No hay tarjeta de "cambio de poder adquisitivo".

**Motivo.** Las dos lecturas no empatan, y la diferencia es el pareo: el nivel sale de `Precio
promedio`, que es transversal; la serie del encadenado sólo compara tiendas presentes en las dos
puntas de cada eslabón. La gráfica usa `Índice encadenado` y no el promedio rebasado porque es la
serie del proyecto (#19) y ya comparte eje con el INPC en P1: rebasar el promedio publicaría, a un
clic de distancia, dos cifras distintas para lo mismo sin decir por qué difieren. Una tarjeta
única tendría que elegir entre las dos sin poder justificarlo en su espacio, que es justo el tipo
de número que este reporte no publica. El lector hace la resta.

### 30. La cobertura del panel se mide por eslabón, no sobre el periodo

**Decisión.** P5 publica la mediana por eslabón de la razón tiendas pareadas sobre observadas, por
tienda, cadena, municipio y estado. El eje sale de `Nivel de cobertura`, una tabla desconectada de
cuatro constantes leída con `SWITCH`. El municipio se cuenta junto con su estado.

**Motivo.** La razón sobre el periodo entero contesta "¿alguien quedó excluido para siempre?", que
es la pregunta menor: el índice se calcula eslabón a eslabón, así que lo que hay que declarar es
cuánto usa cada comparación. Y la cifra del periodo mejora sola con el tiempo sin que el método
mejore en nada. Tabla desconectada porque son cuatro columnas de `dim_tienda` y no cuatro valores
de una, y Direct Lake no admite columna calculada (#21, #22, #24). El municipio con su estado
porque los nombres se repiten entre estados.
