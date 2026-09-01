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
