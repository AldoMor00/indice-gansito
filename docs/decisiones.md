# Decisiones

Qué se decidió, qué se descartó y por qué. Donde algo no se haría así en producción,
se dice.

## 1. La zona raw vive fuera de Fabric

El histórico se guarda en `indice-gansito-datos`, no en OneLake, porque la capacidad es
una trial y va a desaparecer con sus datos. Con el raw afuera, Fabric queda desechable:
se borra el workspace y se reconstruye sin perder historia. En producción viviría en
ADLS; git no es un almacén de datos.

## 2. Se filtra en la puerta

De cada CSV de ~155 MB se persisten dos cortes: las filas del catálogo objetivo, y las
tuplas distintas de tienda —estas del archivo **completo**, para que `dim_tienda` no
quede sesgada a las tiendas que venden pastelillos.

Rompe la inmutabilidad del raw y es la concesión más grande del proyecto. Se mitiga
guardando el `sha256` y la URL de origen en el manifiesto, para poder rehacer cualquier
corte desde la fuente.

## 3. Los notebooks no usan lakehouse por defecto

El enlace del UI guarda el GUID del lakehouse: al desplegar a otro workspace sigue
apuntando al origen y el notebook corre en verde sobre los datos equivocados. Los
notebooks leen su workspace en tiempo de ejecución y arman la ruta con nombres. Lo que
sí hay que reasignar —pipelines, modelo semántico, reporte, conexiones— vive en
`fabric/parameter.yml`.

## 4. Prod no está conectado a git

Sólo `ws-gansito-dev` tiene git integration. A prod se le despliega desde `main` con
`fabric-cicd` y credencial federada OIDC, sin secretos guardados. Rollback = revertir el
commit. `unpublish_all_orphan_items` no se llama porque borraría cualquier `Report` o
`SemanticModel` creado a mano en prod: la rama no representa el estado deseado completo.
No es que pueda llevarse un lakehouse —eso está cubierto dos veces, por el alcance del
despliegue y por el feature flag `enable_lakehouse_unpublish`.

`Lakehouse` está fuera del alcance por otra razón: los tres de prod se crean a mano y son
los dueños de los datos; el CI no tiene por qué administrarlos.

El costo es que prod acumula huérfanos: un item borrado en dev sigue vivo en prod
hasta que alguien lo borre a mano. Se prefiere limpiar basura manualmente a arriesgar
un borrado destructivo automático.

## 5. Dos ambientes, sin test

Dev clona los lakehouses de prod con `SHALLOW CLONE`: se copia el metadato, no los datos, y
aun así el clon es escribible y aislado —lo que se le escribe crea archivos propios y no
toca el origen—. Medido: 213,772 filas clonadas entre lakehouses y entre workspaces, sin un
solo parquet propio y con el origen intacto después de escribirle.

El clon es el estado inicial, no el destino: dev arranca igual que prod y re-corre sólo la
capa que desarrolla, sin reconstruir lo que no está tocando. Como las tablas se llaman igual
y viven en la misma ruta, el notebook no parametriza nada y el mismo código corre en los dos
ambientes; un shortcut no daría eso, porque es de sólo lectura. A cambio, el clon es una
foto —se refresca re-clonando— y un `VACUUM` en prod puede romperlo. Es la letra chica del
clon zero-copy de Snowflake y del shallow clone de Databricks.

## 6. Dos modelos semánticos

Direct Lake exige capacidad y no se puede mover a una cuenta gratuita. El de Fabric es
Direct Lake sobre `lh_gold`; el público es un PBIX en modo import que lee los agregados
exportados a CSV por URL anónima. El costo es que el DAX vive duplicado.

## 7. Nada de wheels: `%run` y pruebas en notebook

Publicar una wheel a un Environment de Fabric toma minutos y mata la iteración. Los
notebooks comparten helpers con `%run nb_00_config` y las pruebas de código van en
`nb_90_pruebas`. Las de datos son las compuertas de la decisión #16, dentro del notebook que
escribe la tabla: son precondiciones de esa escritura y por eso viven junto a ella.

El Python que corre en GitHub Actions es caso aparte: nunca entra a Fabric, así que ahí
sí hay `pytest` y `ruff` normales.

El grueso del esfuerzo de pruebas va sobre **datos**, no sobre código. La lógica pura de
un pipeline es poca y sus errores salen a la primera corrida; los incidentes de verdad
vienen de la fuente —una columna que cambia, un lote a medias, un null donde nunca hubo—.
Por eso `nb_90_pruebas` se queda chico y se corre a mano con `%run`, y lo sistemático es
validar cada corrida. La observabilidad —tasas, deriva, los conflictos de mismo día— **no
lleva notebook propio**: son métricas del lote que el notebook que escribe ya tiene en
memoria, y salen por `apunta()` al resumen de la corrida. Uno aparte tendría que releer
bronze entero para recalcular lo que aquí ya está calculado.

Cada tipo de fallo se trata distinto, y esa es la parte que no se improvisa:

- **código roto** → no se commitea;
- **carga incompleta** → truena el job. Bronze reconcilia su conteo por
  `(quincena, intento)` contra `filas_filtradas` del manifiesto, y un descuadre es un
  `raise`: no es un dato malo, es un pipeline roto;
- **dato malo** → no existe como categoría propia: truena igual que una carga incompleta
  (decisión #15). El pipeline no distingue entre "la fuente cambió" y "el pipeline se rompió",
  porque el arreglo es el mismo en los dos casos y lo hace la misma persona.

## 8. Bronze lee por HTTPS, no por shortcut

Los shortcuts de OneLake hablan ADLS, S3, GCS, Blob, Dataverse y OneDrive; GitHub no está
en la lista. El notebook baja los parquets de `raw.githubusercontent.com`, que sirve el
repo anónimo, y no lista directorio: `profeco/manifiesto.jsonl` es el índice y las rutas
se derivan de `(quincena, intento)`.

Bronze aterriza en `Tables/dbo` como Delta y `Files` se queda vacío. La copia ya existe
—bronze no castea, así que la tabla contiene lo mismo que el parquet— y una tercera en
`Files` no podría ser fuente de verdad, porque vive en la capacidad que la decisión #1 da
por desechable. Cómo llega ese bronze a dev es la decisión #5. Y se escribe con
`mergeSchema` apagado: el esquema de los 46 parquets está medido y es idéntico, así que
una columna nueva de la fuente es alarma de lote, no algo que se absorba.

En producción esto sería un shortcut a ADLS y `Files` no sería copia sino ventana. Lo que
falta sin él no es "cero copia" —bronze materializa igual— sino que un archivo nuevo
aparezca sin correr nada.

## 9. Cada fuente ingesta por su lado

Un script y un manifiesto por fuente, y en el repo de datos todo cuelga de `profeco/` o de
`conasami/`. Lo único compartido es `descarga()` y `leer_manifiesto()`, en
`scripts/fuente.py`, porque es lo único que se repite.

No se parecen. Profeco entrega lotes grandes e inmutables por quincena, que se cortan en
la puerta (decisión #2). CONASAMI entrega dos archivos de ~20 KB que se reescriben en su
lugar: no hay período, no hay corte, y lo que decide si hay algo que hacer es el `sha256`,
no una etiqueta. Se baja siempre y se escribe sólo si cambió —justo lo que Profeco no
puede hacer, porque serían 7 GB por corrida—. Una versión nueva entra con sufijo `_vN`,
espejo del `_iN` de los reintentos.

El CSV se guarda tal cual, sin convertir a parquet: son 40 KB, no hay nada que cortar ni
que ahorrar, y un parquet no es reproducible byte a byte.

El `sha256` del manifiesto es el de los bytes que sirvió el host, calculado al vuelo
mientras se descarga. Eso es todo lo que necesita el versionado: cada corrida rehashea lo
que baja y lo compara contra el del manifiesto, así que la comparación siempre es
descarga contra descarga y git nunca entra en ese lazo. La copia del repo es para leerla,
no para reverificarla contra ese hash.

## 10. `pl_bronze`: encadenado, con sesión compartida y un nodo de unión

El pipeline nació con las dos actividades sueltas, para que las fuentes fallaran por separado.
No cabe: dispararon en el mismo segundo y una se fue con `430 TooManyRequestsForCapacity`. La
capacidad de trial no da para dos sesiones de Spark, y dev y prod comparten la misma.

Van encadenadas con `on completion` —encadena por capacidad, no por dependencia, así que el
fallo de una no impide que la otra cargue— y comparten sesión con el `sessionTag` `bronze`: la
primera la crea, la segunda se engancha y nunca pide sesión, así que el 430 entre ellas deja de
ser posible. Se adoptó por eso y no por velocidad: en tres corridas el tiempo no mejoró de
forma medible. El reintento se queda para la primera, que sí pide sesión.

`on completion` sola miente: en ADF una actividad que falla y sólo tiene camino de completion
se da por manejada, y el pipeline reporta éxito con una fuente sin cargar. Por eso
`ambas_fuentes_ok`, un `Wait` de un segundo que depende de las dos con `on success`. Las
dependencias múltiples se evalúan con AND, así que un fallo deja su camino de éxito sin tomar y
el pipeline truena, como manda la decisión #7.

El costo es que el tag no basta solo: hay que prender *High concurrency* en los settings de
cada workspace, y eso vive fuera de git. Queda en `fabric/README.md` como requisito de
reconstrucción.

## 11. Los dos workspaces van en la misma versión de runtime

Las tablas nacen en el protocolo (3,7) con `deletionVectors`, y un runtime más viejo no puede
leerlas. Como el `SHALLOW CLONE` de la decisión #5 es lo que pone el bronze de prod en dev, un
workspace rezagado dejaría de poder clonar al otro. El switch del runtime es reversible; el
protocolo de una tabla no, así que el desempate es obvio: se suben los dos o no se sube
ninguno.

El requisito queda en `fabric/README.md` junto a High concurrency, porque vive fuera de git.

## 12. El grano de silver es la quincena, y el precio es un promedio

Silver lee de bronze el `intento` máximo de cada quincena y agrega a tienda-SKU-quincena.
Promediar varias visitas ya es el caso normal —de las 126,493 celdas de la canasta, sólo 49,179
traen una sola observación—, así que los 177 grupos con dos precios distintos el mismo día
dejan de ser un caso especial en cuanto el grano deja de intentar ser diario. No fallan ninguna regla de
calidad: son promociones, alzas cruzadas y precios transitorios, medidos en
[`fuentes.md`](fuentes.md), y ninguno se tira. Cuál de los dos se tome es inmaterial: el bajo,
el alto o el promedio mueven el cambio del Gansito entre la primera y la última quincena 0.07
puntos porcentuales. Se promedia porque conserva las dos observaciones en vez de escoger una
sin criterio.

La columna se llama `precio_promedio`, no `precio`, y va con `observaciones`, `precio_min` y
`precio_max` de la misma agregación. Con eso quien consuma silver sabe si el número se observó
de verdad —`precio_min = precio_max`, cierto en el 92.65% de las celdas— sin una bandera
booleana, que sería derivable de columnas que ya están ahí y habría que mantener al día. Las
observaciones exactas nunca se pierden: viven en bronze, que no filtra ni deduplica, para
quien necesite la serie diaria en vez de la quincenal.

## 13. La identidad sale de la fuente

Una tienda es `(nombre_comercial, direccion)` y un SKU es `(presentacion, marca)`: las únicas
claves mínimas de las 46 quincenas, medidas en [`fuentes.md`](fuentes.md). `producto` y
`categoria` son constantes en las 213,772 filas, y la coordenada no identifica aunque nunca se
mueva. De `presentacion` se parsean piezas y gramaje, que entran como atributos —
`precio_por_gramo` es lo que hace comparable un Gansito de 50 Gr. contra un Panqué de 280—, no
como clave: `(piezas, gramos)` colisiona en los dos Panqués y en las dos Barritas. Que los 11
SKUs parseen es regla de DQ.

La canasta son 9 SKUs en las 46 quincenas: las Barritas Fresa y Piña se excluyen de toda la
serie, porque Profeco las reclasificó a Galletas Dulces y salen del corte en `2025-03_q2`.
La exclusión va en silver y no en `objetivo.yml`, porque el filtro de ingesta es por `producto`
y ahí comparten valor con el resto.

## 14. Un patrón de escritura por tipo de tabla, y nunca `overwrite`

El patrón lo decide la tabla, no el tamaño del dato. A esta escala reconstruir `hechos_precios`
entero tardaría lo mismo que actualizarlo, así que se elige el que se usaría en producción: el
volumen de la muestra es un accidente del dataset público, no el caso que se está resolviendo.

| tabla | patrón | por qué |
|---|---|---|
| bronze | `append`, `mergeSchema` apagado | no castea ni deduplica, y una columna nueva es alarma de lote (decisión #8) |
| dimensión | `MERGE` por clave, con condición de cambio | es acumulativa: una tienda que salió del panel no deja de existir y los hechos viejos la siguen apuntando |
| dimensión SCD2 | `MERGE`, cerrando vigencias con `LEAD` | CONASAMI reexpide la historia completa cada año, así que la vigencia se deriva del lote y el mismo MERGE la cierra |
| hecho | `replaceWhere` sobre las quincenas recalculadas | la quincena está completa o no está; un MERGE leería la tabla entera buscando filas que por construcción no existen |

`overwrite` de la tabla completa no se usa en ninguna. Reescribirlo todo esconde justo lo que
hay que ver —qué cambió en esta corrida— y deja el historial de la tabla sin nada que comparar.

La condición de cambio del MERGE —`NOT (d.col <=> n.col)` sobre los atributos— es la mitad que
carga peso. Sin ella la corrida sin novedades reescribe archivos y el log deja de distinguir
"no pasó nada" de "se recalculó todo"; con ella un MERGE que no cambia nada no commitea versión.

## 15. Silver es fail fast: pasa o truena, sin cuarentena ni bandera

El proyecto nació con `precios_cuarentena` —la fila que no casteaba se apartaba a una tabla
paralela y el pipeline seguía— y se probaron en el camino las dos alternativas intermedias: un
contador de inválidas en el hecho, y una bandera de válido con su motivo en la propia fila. Las
tres comparten el defecto: dejan aterrizar dato que todavía no se confía, y a partir de ahí todo
conteo depende de que alguien se acuerde del `WHERE`. Números que no cuadran es exactamente lo
que este proyecto no puede permitirse, porque lo único que publica son números.

El tradeoff real no es "datos contra nada". `replaceWhere` sólo toca las quincenas pendientes, así
que una corrida que truena deja las anteriores intactas: es **viejo pero cuadrado** contra
**fresco pero descuadrado**, y con un solo consumidor —el índice— y una sola tolerancia, la
bandera no le sirve a nadie. Una bandera gana cuando hay varios consumidores con tolerancias
distintas, o un SLA que hace que el dato parcial valga más que ninguno. Aquí no hay ni uno ni
otro: un operador, y la fuente lleva nueve meses sin publicar.

Lo que se pierde es el registro persistente de qué falló, y se compensa solo: con cuarentena las
filas malas se acumulan calladas durante meses y hacen falta una tabla y una consulta para
encontrarlas; con fail fast la corrida se detiene ahí, en un notebook, sobre un lote conocido y
con la columna nombrada en el error. No hay que buscar porque no te moviste del lugar. Y como
nada se escribió, la quincena sigue pendiente: se arregla el notebook, se vuelve a correr y entra
sola, sin dropear tablas ni parámetro de backfill.

Tampoco hay tier de aviso. La prueba es "¿publicarías el dato con esto sin resolver?": si la
respuesta es no, es un error diferido y es peor que un error; si es sí, entonces no era un aviso
sino un atributo del dato o una métrica del lote, y las dos ya tienen dónde vivir.

Las mediciones respaldan la elección más de lo que la inspiraron: 27.2 millones de filas leídas,
213,772 al corte, y ni un solo valor que no castee. Construir maquinaria para el caso sucio era
construir para algo que no ha pasado nunca.

## 16. Dónde vive cada compuerta, y por qué ninguna se repite

`nb_00_config` enciende ANSI —Fabric lo trae apagado, contra el default de Spark 4—, así que un
`cast` fallido truena y `try_cast` sigue dando nulo. Con eso los dos dejan de ser sinónimos y
pasan a ser una decisión visible por columna: `cast` es "esto tiene que pasar", `try_cast` es
"esto puede faltar". En `nb_20` sobrevive un solo `try_cast`, en `piezas`, donde la ausencia es
legítima. El tipado deja de necesitar compuerta propia.

Lo demás se reparte por lo que cada mecanismo alcanza a ver, y nada se valida en dos:

| compuerta | qué revisa | por qué ahí |
|---|---|---|
| ANSI | que el valor castee | es el cast mismo; no hay nada que escribir |
| entrada, sobre el lote crudo | obligatoria vacía, un atributo por clave, formato del que cuelga un regex | es sobre la **fuente**, y el mensaje —"`direccion`: 12 filas vacías"— es lo que se repara |
| salida, sobre el DataFrame armado | llave sin colisión | sólo existe después de transformar |
| constraint CHECK de Delta | `precio_min > 0`, `gramos > 0`, `piezas > 0` | es predicado de **una fila del resultado**, lo único que Delta sabe expresar |

La corrida va en tres tiempos —armar, validar, escribir— con las escrituras al final. Spark es
perezoso, así que mientras nadie llame `.write` los DataFrames son la etapa de staging: se
validan con sus llaves ya calculadas y una compuerta que truena no deja nada a medias. Es
Write-Audit-Publish sin la tabla intermedia ni el swap.

Las constraints las aplica el notebook y no un DDL a mano, porque una constraint puesta fuera
desaparece al recrear la tabla y una protección que crees tener y no tienes es peor que ninguna.
Se descartaron a propósito las que no pueden fallar: `NOT NULL` sobre un `id` es teatro —
`xxhash64` nunca devuelve nulo, y el riesgo real de esa llave es que sea válida y **equivocada**
cuando la clave natural viene vacía, que es cosa de la compuerta de entrada.

## 17. La zona del salario mínimo son 6 identidades de 7 literales, y su vigencia se cierra global

`resto del pais` (2019-2024) y `general` (2025-) son la misma zona renombrada, medido en
[`fuentes.md`](fuentes.md), así que la identidad va sobre el nombre normalizado y el literal se
queda de atributo: es justo lo que la SCD2 versiona. `unica` (2015-2018) **no** se fusiona con
ellas aunque cubriera el mismo país: al crearse la ZLFN en 2019 se le recortó territorio, y eso
es cambio real y no cambio de nombre. `a`, `b`, `c` son zonas históricas propias.

El cierre de vigencias es **global y no por zona** porque CONASAMI reexpide el tabulador
completo: una vigencia termina cuando entra el siguiente, no cuando esa zona cambia de salario.
Partido por zona, `a`, `b`, `c` y `unica` quedarían abiertas para siempre —su último renglón no
tiene siguiente— y una quincena de 2024 haría match con las seis. El punto ciego es la zona que
sale de un tabulador y vuelve en el siguiente, que abriría un hueco sin salario; en las 42 filas
no pasa nunca ([`hechos.md`](hechos.md)) y una compuerta de `nb_21` truena si empieza a pasar.

La vigencia abierta se cierra con `9999-12-31` y no con nulo: `es_vigente` sale de comparar
contra el centinela, y el `BETWEEN` de gold no tiene que arrastrar un `OR IS NULL`.

## 18. Ni variable library ni user data functions

`fabric-cicd` soporta los dos tipos, así que el descarte es de diseño. La variable library
parametriza lo que cambia entre ambientes, y aquí no cambia nada: mismos nombres y mismas rutas
en los dos workspaces, resueltos en vivo por `ruta_tabla` (regla #1, decisión #5). Encima
cobraría, porque el value set activo es setting del workspace y no viaja en el item.

La user data function centraliza lógica reutilizable, que es lo que hace `nb_00_config`, pero
sin sesión de Spark y por REST, y los helpers toman y devuelven DataFrames: sólo `manifiesto_de`
cruzaría. Paga con write-back translytical o consumidores externos, y no hay ninguno. Lo medido,
en [`hechos.md`](hechos.md).

## 19. El cambio de precio se publica con Jevons encadenado, no con un promedio

El panel de Profeco rota, así que comparar el promedio de la primera quincena contra el de la
última mezcla cambio de precio con cambio de muestra. El índice se arma pareando: cada quincena
contra la inmediata anterior, sobre las tiendas presentes en ambas, y los 45 eslabones se
encadenan. Ninguna tienda necesita sobrevivir dos años para aportar, y se retiene el 86% del n
—el panel balanceado, que parece la corrección obvia, retiene el 9% y sale sesgado por
supervivencia—. Las cuatro variantes con sus intervalos, en [`hechos.md`](hechos.md).

Del par de medias que sobreviven —Dutot y Jevons quedan a medio punto, y Carli se descarta
porque sobreestima 12— se elige **Jevons**: vive en logaritmos, y los logaritmos son aditivos.
Eso es lo que deja que el índice sea una sola medida DAX en vez de una tabla pre-agregada. Y ahí
está la razón de que `hechos_relativos` se materialice **al grano de tienda**: el encadenado no
es aditivo entre cortes —el de «supermercados en Jalisco» no se deriva del de «supermercados»—,
así que pre-agregar eslabones por corte congelaría los cortes para siempre. Al grano de tienda
son ~13,000 filas y cualquier corte sale gratis.

La muestra insuficiente se ataja en la medida y no filtrando gold: el dato entra completo
—filtrarlo sería compuerta de silver en la capa equivocada, y rompería los cortes
transversales, que no necesitan pareo— y el índice devuelve `BLANK()` cuando el eslabón más
flaco del corte no junta el umbral, que arranca en 30 tiendas y se expone como parámetro. No es
lista negra: se evalúa en el contexto de filtro, así que protege igual a una cadena chica, a un
estado chico o a un subperiodo corto. Es lo que decide que el corte de canal sean tres
categorías y no los cinco `giro` de la fuente, de los que sólo uno pasa el umbral.

## 20. El intervalo del índice se materializa en gold, tabulado por SKU

El IC 95% no se puede calcular en DAX. El error del encadenado no es función de los agregados
del eslabón: las mismas tiendas reaparecen eslabón tras eslabón y sus errores se telescopan, así
que sumar varianzas en cuadratura da 2.62 pp contra los 1.60 del bootstrap. Una banda calculada
así saldría 64% más ancha de lo que dicen los datos, que es peor que no publicarla.

`nb_30_gold` lo resuelve con el mismo bootstrap de la exploración —2,000 réplicas, semilla fija
para que dos corridas publiquen el mismo intervalo— y deja `hechos_ic_indice`. Remuestrea
**tiendas** y no celdas, porque la tienda es la unidad de muestreo de Profeco y las celdas de una
misma tienda no son independientes; el sorteo se expresa como pesos multinomiales, que es el
mismo bootstrap escrito de forma que el join lo pueda aplicar sin explotar.

El grano es **SKU × quincena**, no quincena. El índice que se publica es el de un SKU —el
titular es el Gansito, no la canasta— y el intervalo de la canasta no se le parece: promedia
nueve series y sale en ±0.37 pp contra los ±1.60 del Gansito. Cada SKU se remuestrea contra su
propio padrón de tiendas, porque el n del que depende el ancho es el suyo. Entre SKUs no se
agrega, así que la medida exige un solo producto en contexto y se apaga ante cualquier filtro de
tienda: el bootstrap corrió sobre el padrón entero y no sobre el corte.

Es la única tabla de gold que no proyecta silver, y por eso se calcula al final y sobre gold ya
escrito: cada quincena nueva alarga la cadena y mueve el intervalo de todas las anteriores.

## 21. El nombre comercial del SKU se deriva en gold, con diccionario

Profeco declara los nueve SKUs con el mismo `producto` —"Pastelillos y Pan Dulce
Empaquetado"— y la misma `categoria`, así que lo único que los distingue es la cadena cruda de
`presentacion` (decisión #13). El reporte necesita "Gansito", no `Paquete con 1 Gansito
(50 Gr.)`, así que `dim_producto` gana una columna `nombre`.

Diccionario explícito y no regex: dos de los nueve no siguen el patrón `Paquete con N <nombre>
(M Gr.)` sino `Paquete 280 Gr. Panqué ...`. Y a diferencia de `CANAL`, aquí el diccionario es lo
correcto y no la excepción, porque la canasta está cerrada: una presentación nueva es una alarma,
no un caso que absorber.

La compuerta mira el universo completo de la dimensión y no sólo lo que trae precio. Silver ya
ataja el SKU nuevo —truena si el lote pasa de nueve presentaciones— pero no ve el **renombre**:
con nueve presentaciones y el regex casando, pasa limpio, pero la clave es `xxhash64` sobre la
cadena, así que nace otro `id_producto` y la dimensión, que es acumulativa, queda con la vieja y
la nueva. Llegaría aquí sin nombre y `coalesce` la dejaría en nulo: un blanco en el slicer y P1
sin encontrar su producto. `nombre` y no `nombre_comercial`, que ya es media identidad de una
tienda.

## 22. El histograma sale de una tabla de bins desconectada

Power BI no trae histograma nativo, y las salidas del UI tampoco sirven aquí: el binning
—*New group → bin size*— crea una **columna calculada**, y las tablas Direct Lake no las
admiten. Las alternativas eran binear en gold, que congela el ancho del bin dentro de los datos
y obliga a recorrer `nb_30` para cambiarlo, o un visual de AppSource, que mete una dependencia
externa en una pieza que se va a enseñar. Se elige una **tabla de constantes desconectada**,
`Rango de precio`: no referencia Direct Lake y por eso es legal, y es la forma que el modelo ya
usaba para `Umbral de pareo`.

Bins de un peso con el borde en el medio peso. El ancho no es gusto —Freedman-Diaconis sobre el
corte real pide $0.99— y el borde resuelve algo concreto: los precios de anaquel se apilan en
pesos redondos, y con el corte en `x.50` la moda cae al centro de la barra en vez de partirse
entre dos. `Tiendas en rango` devuelve **cero adentro del rango observado y `BLANK` afuera**;
sin esa rama la serie sale con 76 barras y ceros a los lados, y devolviendo `BLANK` a secas los
bins vacíos de en medio se colapsan y la forma del histograma miente.

## 23. Los cortes transversales llevan un mínimo de cinco tiendas, y va en el visual

P2 no compara periodos, así que no hay pareo ni guarda del índice (decisión #19) y los cinco
giros de la fuente valen completos. Pero ordenar por precio con la muestra entera pone el ruido
arriba: la cadena más cara de la última quincena es una farmacia con dos tiendas. El mínimo de
cinco es un **filtro de visual** y no un filtro de gold, por la misma razón que la guarda del
índice —el dato entra completo y quien decide es la medida en su contexto—, y por eso convive
con visuales de la misma página que no lo llevan.

Donde más pesa es en el mapa: un coropleta pinta área, y el área se lee como peso. Un estado con
una sola tienda saldría del color más intenso del mapa. Los que no alcanzan el umbral se quedan
sin pintar, que es lo que de verdad dicen los datos.

La tabla de municipios lleva el estado como columna aparte, y eso no es formato: los nombres de
municipio se repiten entre estados, así que agregarlos por nombre inventa una fila.

## 24. La clave del mapa se deriva en gold, con diccionario

El mapa es un `shapeMap` y no Azure Maps. Azure Maps sólo opera en las regiones de Estados
Unidos y la Unión Europea; fuera de ahí hace falta encender además el switch que deja procesar
los datos fuera de la región del tenant, y una pieza de portafolio no debería depender de dos
palancas de administración ni de que el dato salga del tenant para dibujarse. Además dibuja
tiendas y no estados, y las burbujas se amontonan en el Valle de México. Se paga que `shapeMap`
siga en preview.

`shapeMap` casa la ubicación contra las claves de su TopoJSON. 28 de los 30 estados que declara
Profeco casan por nombre —la fuente los escribe sin acentos, igual que el mapa—, pero los dos
que no casan son los dos más grandes del padrón. De ahí `clave_estado` en `dim_tienda`, con el
mismo patrón de la decisión #21: diccionario explícito, los 32 y no los 30 observados para que
Colima y Nayarit pinten el día que aparezcan, y compuerta que detiene la corrida ante un estado
que no esté. Se guarda el ID del TopoJSON y no el ISO ni el nombre en inglés, porque es la llave
del propio mapa; `estado` se queda como lo escribe Profeco, igual que `giro` junto a `canal`.

Agregar la columna obligó a **reconstruir** `dim_tienda` en gold, no a evolucionar su esquema:
el MERGE de `upsert` exige que origen y destino tengan las mismas columnas. La alternativa
—`autoMerge` de sesión— la desaconseja la propia documentación, y convertiría cualquier cambio
de esquema futuro en algo que entra callado, que es lo contrario de cómo este repo los trata.
Gold es una proyección de silver: se dropea la tabla y se vuelve a correr.

## 25. Por cadena se publica nivel, no índice

El plan de la fase le daba a P3 tres visuales, y el tercero eran barras de cambio encadenado por
cadena. No existe: medido contra el modelo, **ninguna de las 37 cadenas alcanza el umbral de
pareo**. La mejor es Hipermercado Soriana con 26 tiendas pareadas en su peor eslabón contra las
30 que exige la guarda (decisión #19), y de ahí para abajo Wal-mart 20, Chedraui 16 y Bodega
Aurrera 15. El visual habría salido en blanco las 37 veces.

Es el mismo resultado que ya había dado el `giro` crudo de Profeco y que obligó a colapsar a tres
canales: **el umbral decide la granularidad publicable, no el gusto**, y la cadena queda por
debajo de esa línea para cualquier índice.

Lo que sí sostiene es el **nivel**: precio al inicio contra precio al cierre, que no encadena
nada y por eso no le debe nada al pareo. Ahí la guarda que aplica es la de los cinco (decisión
#23), con una vuelta de tuerca —cinco tiendas **en las dos puntas**, no en el promedio de las
dos, porque con una sola punta pasan cadenas que en la otra no existen—. Quedan 12 cadenas. Por
eso la página cierra con dos visuales y no con tres, y el dumbbell se lleva el ancho que sobró.

El precio de la elección se dice en la página y no en una nota al pie: las dos puntas no son las
mismas tiendas, así que el dumbbell mide nivel de cadena y no cambio pareado.

## 26. El dumbbell se dibuja con error bars, no con un visual de AppSource

Power BI no trae dumbbell, y la pieza es la que carga el hallazgo de P3: dónde arrancó y dónde
terminó cada cadena, en un solo renglón. Las salidas eran tres. Un visual certificado de
AppSource mete una dependencia de tenant en un reporte que se escribe entero a mano, y que la
copia import de la decisión #6 tendría que renderizar también. Un scatter de inicio contra
cierre con *symmetry shading* es nativo y analíticamente más fuerte —la distancia a la diagonal es el
encarecimiento— pero se lee frío y encima con doce etiquetas. Barras pareadas se leen solas y
pierden la línea que une las dos puntas, que es lo que hace legible la brecha.

Se arma con lo que el reporte ya tenía probado: **una barra clusterizada con error bars**, que la
documentación soporta en barra y columna clusterizada, línea y combo. El valor es `Precio al
inicio`, las cotas son `Precio al inicio` y `Precio al cierre` con `isRelative: false`, y los
caps encendidos son las dos pesas —al revés que la banda de P1, donde con 46 puntos tapaban el
relleno y hubo que apagarlos—. La barra se apaga con `fillTransparency` al 100, que es propiedad
del punto: apagarla pintándola del color del lienzo la habría dejado dependiendo del tema.

Dos detalles que no son cosméticos. El eje va fijo de $12 a $24, porque arrancando en cero las
doce mancuernas se apelmazan en el cuarto derecho. Y el orden es por `Precio al inicio`, que es
lo que deja ver que los que arrancaron baratos son los que más subieron; ordenando por el cierre
las longitudes se revuelven y el patrón se pierde.

## 27. El índice no publica 100 en la quincena base sin observación

`Índice encadenado` devuelve 100 en la quincena base por definición: no hay eslabón anterior
contra el cual parear, así que la guarda no tiene qué exigirle, y sin esa rama la serie arranca
en blanco y entra un eslabón tarde. Pero la rama estaba escrita sin condición, y publicaba ese
punto para cualquier corte, incluso uno sin una sola celda detrás.

Se vio al partir por canal. `canal` es nulo para los giros que no venden pastelillos —1,429
tiendas del padrón, cero celdas en `hechos_precios`— y ese grupo dibujaba una cuarta serie
fantasma en la leyenda: un punto en 100 y nada más.

Se arregla en la medida y no con un filtro de visual. El filtro tapaba el caso en esa página; la
medida lo resuelve en cualquier corte que venga después, y P5 parte por nivel de cobertura. La
rama pide ahora `NOT ISBLANK( [Precio promedio] )`: 100 por definición sigue pidiendo que haya
algo definido. Las cuatro cifras de F4 no se movieron.

## 28. El miembro en blanco se filtra en el slicer, no se promete integridad

Los slicers mostraban una opción en blanco, y no son datos sucios: `dim_producto` tiene 9 filas y
ninguna con nombre nulo, y las tres tablas de hechos apuntan todas a un producto que existe —el
conteo no baja ni una fila al excluir la fila en blanco—. Es el *unknown member*, la fila virtual
que el motor agrega del lado "uno" de cada relación regular al expandir con `LEFT OUTER JOIN`, y
que en Direct Lake se queda puesta porque la integridad no se puede validar contra los Delta.
Está en las tres dimensiones: producto da 10 miembros contra 9 filas, `cadena_comercial` 257
contra 256 y quincena 47 contra 46.

Se quita con un filtro de visual "is not blank" en cada slicer. La alternativa era marcar las
relaciones con *Assume referential integrity*, que en Fabric sí se puede y de un golpe cambia el
join a `INNER` y borra el miembro de todo el modelo. No se hace: es una promesa sobre los datos,
y el día que se rompa —una llave de hecho sin su fila de dimensión— esas filas desaparecen
calladas en vez de acumularse en el blanco. Silver está construida al revés (decisión #15), y el
índice publica en blanco antes que publicar de más.
