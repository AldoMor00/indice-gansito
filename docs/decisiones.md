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
notebooks comparten helpers con `%run nb_00_config`; las pruebas de código van en
`nb_90_pruebas` y las de datos en `nb_40_dq`, que escribe su veredicto en
`dq_resultados` en vez de tronar con un assert suelto.

El Python que corre en GitHub Actions es caso aparte: nunca entra a Fabric, así que ahí
sí hay `pytest` y `ruff` normales.

El grueso del esfuerzo de pruebas va sobre **datos**, no sobre código. La lógica pura de
un pipeline es poca y sus errores salen a la primera corrida; los incidentes de verdad
vienen de la fuente —una columna que cambia, un lote a medias, un null donde nunca hubo—.
Por eso `nb_90_pruebas` se queda chico y se corre a mano con `%run`, y lo sistemático es
validar cada corrida.

Cada tipo de fallo se trata distinto, y esa es la parte que no se improvisa:

- **código roto** → no se commitea;
- **carga incompleta** → truena el job. Bronze reconcilia su conteo por
  `(quincena, intento)` contra `filas_filtradas` del manifiesto, y un descuadre es un
  `raise`: no es un dato malo, es un pipeline roto;
- **dato malo** → cuarentena, y el pipeline sigue (regla dura #3). Tirar un lote de 4,530
  filas por 12 inválidas cuesta más de lo que evita.

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
`precios_cuarentena` se reescribe con el mismo `replaceWhere` que el hecho, para que las dos
tablas nunca queden de corridas distintas (regla dura #3).
