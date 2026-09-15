# Las fuentes

Qué son los archivos que se ingestan y qué se puede dar por cierto de ellos, antes de tocarlos.
Lo que **hacemos** con ellos no está aquí: vive en `scripts/`, en `objetivo.yml` y en
[`decisiones.md`](decisiones.md). Cada fuente lleva las mismas cinco secciones: dónde está, qué
trae, identidad y grano, defectos conocidos, y lo que la fuente dice de sí misma.

Las tres son datos abiertos del Gobierno de México. Ninguna expone listado de directorio: la
única forma de saber qué hay publicado es pedirlo.

---

# Profeco — *Quién es Quién en los Precios*

## Dónde está

Un bundle por año en el portal de datos abiertos de Profeco:

```
datos.profeco.gob.mx/datos_abiertos/qqp.php            listado
datos.profeco.gob.mx/datos_abiertos/file.php?t=TOKEN   el bundle de un año
```

Publica **mensual**, con un mes de rezago y sin día fijo; julio de 2026 salió el 31 de agosto. Lo
que hay que saber para automatizarlo:

- **El `TOKEN` es una cadena de 32 hexadecimales asignada a mano**, no derivable: la única forma
  de conocer el de un año es leerlo del listado, que es HTML servido por PHP sin XHR ni JSON
  detrás.
- **Se baja entero o nada.** El bundle ignora `Range`, no manda `Content-Length`,
  `Last-Modified` ni `ETag`, y lleva `Cache-Control: no-store`. No hay forma barata de
  preguntarle si cambió. Lo que sí hay es un CSV de metadatos de 877 bytes que declara
  `Cobertura temporal` y `Última modificación`.
- **Un token inválido contesta `200` con HTML**, no `404`: una página con un
  `alert('Documento no disponible')`. Hay que ver que lo que llegó sea un zip.
- **El zip trae su propio manifiesto**: el directorio central lleva tamaño y CRC32 de cada
  miembro, legibles sin descomprimir.
- **No pide nada**: ni cookie, ni referer, ni `User-Agent`.
- **2025 viene en `.rar`**; 2024 y 2026 en `.zip`. 2023 no tiene bundle.
- **El nombre del archivo cambió de convención**: `MM-AAAA_01`/`_02` hasta 2025, `MM-AAAA_Q1`/`_Q2`
  desde 2026.

Hasta `2025-11_q2` el mismo archivo lo servía `repodatos.atdt.gob.mx`, un CSV por quincena. Ese
host contesta 503 desde entonces. Verificado en septiembre de 2026: las 46 quincenas bajadas de
ahí tienen el mismo `sha256`, byte por byte, que sus copias dentro de los bundles del portal. El
cambio de canal no partió la serie (decisión #34).

## Qué trae

15 columnas idénticas entre años, todas texto:

`producto, presentacion, marca, categoria, catalogo, precio, fecha_registro, cadena_comercial,
giro, nombre_comercial, direccion, estado, municipio, latitud, longitud`

Entre 140 y 225 MB y entre 437 y 710 mil filas por archivo. Normalmente utf-8 con BOM, CRLF y
comas embebidas entre comillas. `fecha_registro` es `yyyy/MM/dd`, sin hora. El corte de quincena
es 1–15 / 16–31.

Lo que no es obvio de las columnas:

- **`producto` es el genérico, no el nombre comercial.** El Gansito está en `presentacion`
  (`Paquete con 1 Gansito (50 Gr.)`) y Marinela en `marca`; buscar "Gansito" en `producto` no
  devuelve una fila. Por eso `objetivo.yml` filtra por `producto`.
- **`S/m` aparece sólo en `marca`**, en un tercio de las filas, y significa "sin marca": granel
  legítimo, no un centinela de nulo.
- **`(producto, presentacion, marca)` no es estable en el tiempo.** Profeco reclasifica y recorta
  gramajes: las Barritas Marinela pasaron de Pastelillos a Galletas Dulces entre 2024 y 2025, y el
  Oreo bajó de 273.6 a 252 Gr. Para silver son productos nuevos.
- **`catalogo` se acentuó en 2026** (`Basicos` → `Básicos`, `Pacic` → `PACIC`). No toca la
  identidad, pero parte cualquier agrupación por ese campo que cruce el año.

## Identidad y grano

Medido sobre las 46 primeras quincenas completas, en agosto de 2026, y confirmado por las
llaves internas que la fuente dejó ver en junio de 2026.

- **La clave de una tienda es `(nombre_comercial, direccion)`, y es la única.** Búsqueda
  exhaustiva de los 255 subconjuntos de los 8 campos: ese par no colisiona una sola vez y es el
  único mínimo que lo logra. `direccion` es la del inmueble (Sears y Liverpool comparten la de la
  plaza) y `nombre_comercial` distingue al inquilino; ninguno alcanza solo. El `folio` interno
  que la fuente publicó en junio de 2026 confirma el par uno a uno.
- **`(latitud, longitud)` no identifica, pero nunca se mueve.** Profeco geocodifica el mercado,
  no el local: hasta veinte tiendas bajo un mismo punto, y una de cada diez filas cae en una
  coordenada compartida. A cambio, cada tienda conserva su coordenada exacta en todas sus
  quincenas. Es atributo geográfico, no identidad.
- **Un SKU es `(presentacion, marca)`.** `cv_marca`, la llave interna de junio, es el SKU y no la
  marca: nueve valores para los nueve SKUs del corte.
- **Una visita no es una quincena.** Profeco visita la misma tienda hasta cinco veces por
  quincena, y como `fecha_registro` no trae hora, las visitas de un mismo día no se pueden
  ordenar. `catalogo` no desempata. Un puñado de veces (menos del 0.2 % de las filas) el mismo
  día, tienda y SKU traen dos precios distintos; en casi todos los casos al menos uno es un
  precio que esa tienda cobra en su propia serie. Qué los produce no se sabe, y el grano
  quincenal los promedia (decisión #12).
- **Bajo la clave no cambia ningún atributo.** Cadena, giro, estado, municipio y coordenada son
  constantes por tienda en toda la serie, una vez normalizada la grafía. Lo que sí rota es el
  panel: una de cada cuatro tiendas aparece en todas las quincenas.
- **La muestra no es aleatoria.** Ver "Lo que Profeco dice de su método", abajo.

## Defectos conocidos

Todos llegaron con el lote que empieza en `2025-12_q1`; las 46 quincenas anteriores no traen
ninguno. Cada uno con su fecha, qué lo cazó, y dónde se trata.

| cuándo | qué | qué lo cazó | dónde |
|---|---|---|---|
| `2026-04_q2` en adelante | tiendas de alta **sin coordenada** (siete Bodega Aurrera del Valle de México) | la compuerta de completitud de silver | #38 |
| `2026-05` (dos quincenas) | archivo en **cp1252 sin BOM**; los otros 60 son utf-8 con BOM. Leído como utf-8 lossy, cada byte inválido se vuelve `U+FFFD` y `Panqué` entra como `Panqu?` sin que nada falle | la compuerta de silver que cuenta presentaciones, tres capas después: la canasta de 9 se veía como 11 | #37 |
| `2026-06` (dos quincenas) | **18 columnas en vez de 15**: `folio`, `cv_producto` y `cv_marca`, las llaves internas. Julio volvió a 15 | bronze, al escribir con `mergeSchema` apagado | #36 |
| desde `2025-12_q1`, y en la historia | la **misma tienda con distinta grafía**: con acento y sin él, oscilando entre quincenas, y con el acento perdido como `?`. Miles de identidades de más (`mediciones.md`) | una pregunta sobre desempatar por recencia, no una corrida | #39 |
| en el mismo año | la fuente se **contradice sola** en cinco pares: `Central de Abasto` / `Central de Abastos` en `cadena_comercial`, `Tienda` / `Tiendas Departamentales` en `giro`, y tres que sólo cambian una mayúscula. No se desempatan por frecuencia: la más frecuente es la mal escrita en tres de los cinco | inventario a mano de los no-ASCII | #39 |
| `2026-05_q1` | un **giro nuevo con precio**: dos tortillerías de San Luis Potosí con ocho de los nueve SKUs | la compuerta de canal de gold | `nb_30`, `CANAL` |

Dos caracteres de la grafía que conviene tener nombrados: `ð` es una `ñ` mal decodificada (0xF0
por 0xF1, en `Pescaderia Muðoz`) y `´` es un acento suelto usado como apóstrofo (`O´farril`).
Que el BOM sea un discriminador perfecto de la codificación en estos archivos es coincidencia y
no se usa como tal (decisión #37).

Lo que vale como aviso general: **una fuente puede corromperse sin que nada falle**. Ni la
descarga, ni el hash, ni el esquema, ni el conteo de filas se quejaron de mayo; lo cazó una regla
de calidad que contaba SKUs.

## Lo que Profeco dice de su método

No hay nota metodológica pública. El portal documenta la cobertura (más de 2,000 productos en
poco más de 1,450 establecimientos de hasta 54 ciudades, actualizado todos los días hábiles) y
describe la muestra como "los establecimientos más destacados de cada ciudad". No hay marco
muestral ni diseño probabilístico declarado.

Y una advertencia explícita:

> Los precios sirven exclusivamente como referencia de compra para el consumidor, por la
> metodología utilizada no permiten medir la inflación que se registra en el país.

Profeco añade que "la única autoridad facultada para determinarla es el INEGI". Este proyecto no
publica inflación: publica el cambio de precio de un producto concreto, medido por pareo de
tiendas, y lo deflacta con el INPC de INEGI. La advertencia es de la fuente y va citada.

Consultado el 2026-09-09 en <https://www.profeco.gob.mx/precios/quienesquie_nvo.asp>.

---

# CONASAMI — salario mínimo

## Dónde está

```
repodatos.atdt.gob.mx/api_update/conasami/salarios_minimos/NOMBRE.csv
```

Sin token. De los cuatro archivos del catálogo se ingestan dos: `sm_real_indice` (el nominal
mensual) y `sm_general_profesionales_zonas` (el salario vigente por zona). `sm_historico_anual` y
`sm_general_profesionales_capital` no responden ninguna pregunta del proyecto.

El CDN contesta 403 si la petición no trae `Accept`, y 503 (no 404) por un archivo que no existe.

## Qué trae

**`sm_real_indice.csv`**: una fila por mes desde 1969, unos 18 KB.
`anio, mes, smg_nominal, smg_real, smgr_indice`

**`sm_general_profesionales_zonas.csv`**: una fila por `(inicio_vigencia, zona_salarial)` desde
2009, unos 20 KB. `inicio_vigencia, zona_salarial, salario_minimo_general` y 86 columnas más, un
salario profesional por oficio.

## Identidad y grano

- **La serie mensual crece en tandas anuales.** El catálogo la declara `Anual` y así se
  comporta: publican la serie completa una vez al año, cuando entra el salario nuevo. Hoy llega a
  `2026-01`, así que no cubre la ventana de precios (decisión #40).
- **Hay tres cifras de salario para un mismo año y no se contradicen**: son tres conceptos. El
  de `..._zonas` es el salario **por zona**; `smg_nominal` es el ponderado nacional **mensual**;
  el del histórico anual es el ponderado **anual**. Los dos últimos difieren en centavos. El que
  de verdad se paga en una zona vive sólo en `..._zonas`.
- **`zona_salarial` se renombró en 2025**: `resto del pais` en 2023 y 2024, `general` desde 2025.
  Es la misma zona, la no fronteriza. `zlfn` no cambia de nombre (decisión #17).
- **El tabulador no deja huecos**: cada zona que sale del archivo es una fusión y ninguna vuelve
  (`mediciones.md`).
- **`smg_nominal / smg_real` es el INPC entre 100**, mensual. Ya no se usa como deflactor
  (decisión #35).
- **Cruzar la zona contra el precio exige un catálogo que no está aquí**: la Zona Libre de la
  Frontera Norte se define por municipio y el archivo no los lista.

## Defectos conocidos

Ninguno. Los 86 salarios profesionales traen celdas vacías, pero no entran.

---

# INEGI — INPC quincenal

## Dónde está

La API de indicadores, que pide token:

```
inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/910420/es/00/false/BIE-BISE/2.0/{token}?type=json
```

`910420` es el **nivel del índice** del INPC general, base 2018=100, grano **quincenal**, desde
1988. La respuesta pesa unos 140 KB y es estable byte a byte entre llamadas: no trae marca de
tiempo de la petición, así que el `sha256` sirve para decidir si hay algo que escribir.

Lo que costó encontrarla, porque la documentación de INEGI sólo trae ejemplos de población y los
tres errores dan **el mismo** `400` con "No se encontraron resultados", indistinguible de un
token inválido:

- La fuente es **`BIE-BISE`**, no `BIE`.
- La geografía nacional es **`00`**, no `0700`, que aparece en ejemplos viejos.
- El id no se deduce. Salió de recorrer el catálogo del navegador de indicadores; se reconoce por
  tema `189128` ("Índice"), unidad `1051` ("Índice base 2018=100") y frecuencia `15`
  ("Quincenal"). Casi todos sus vecinos de id son variación porcentual, no nivel: `910445`, por
  ejemplo, es la inflación quincenal anual.

`false` en la penúltima posición pide la serie completa; con `true` devuelve sólo la última
observación. La descarga sin token existe (`inpc_indicador_mensual_csv.zip`) y no sirve: su
última observación es de julio de 2024.

## Qué trae

Una observación por quincena con `TIME_PERIOD`, `OBS_VALUE` y `LASTUPDATE`, más `FREQ` y `UNIT`
de la serie.

## Identidad y grano

- **El tercer campo de `TIME_PERIOD` es el ordinal de la quincena, no el día**: `2026/08/02` es
  la segunda quincena de agosto. Todas las observaciones tienen la forma `AAAA/MM/0Q`.
- **El mensual es el promedio de sus dos quincenas**, verificado contra el que CONASAMI traía
  (`mediciones.md`).
- **Los decimales de la historia son reales, no ruido.** De 2024 en adelante INEGI publica tres
  decimales y el resto es serialización de flotante, pero la serie rebaseada de antes de 2000
  trae decimales de verdad. `decimal(12,6)` deja exacta la ventana del índice.
- **`LASTUPDATE` dice cuándo la actualizó INEGI**, que no es lo mismo que hasta cuándo llega.

## Defectos conocidos

Ninguno.

---

# Apéndice: la API del portal de Profeco, que no se usa

Investigada en septiembre de 2026 y descartada (decisión #36). Está aquí por si algún día Profeco
publica sus llaves en los archivos masivos.

El portal de consulta, `qqp.profeco.gob.mx`, tiene su propia API JSON, sin token ni cookie. Las
rutas salen del bundle de la aplicación; ninguna está documentada.

| ruta | qué devuelve |
|---|---|
| `/api/catalogo` | los 9 catálogos |
| `/api/productos/{clave}` | 12,178 productos con `CVE_PRODUCTO`, `CVE_MARCA`, `MARCA`, `PRESENTACION` |
| `/api/establecimientos` | 7,607 filas con `folio`, `establecimiento`, `cadena_comercial`, `entidad` |
| `/api/producto?tipo=...` | precios **del día**, con dirección, colonia y CP |

Son las mismas llaves que se filtraron en junio: el Gansito es `CVE_PRODUCTO='0008'` y
`CVE_MARCA='002'` ahí, y fue `cv_producto=8`, `cv_marca=2` en el archivo.

Por qué no resuelve nada: no mete las llaves en la historia (los archivos masivos siguen sin
traerlas, y unir por texto falla en uno de nueve SKUs: `PAQUETE C/8. DONITAS. ESPOLVOREADAS (140
GR.)` contra `Paquete con 8 Donitas. Espolvoreadas (140 Gr.)`); `/establecimientos` no sustituye
a `dim_tienda` (sin dirección, con folios repetidos, y sólo el padrón actual); y el endpoint de
precios es más fresco pero sin llaves ni historia. Además es una API interna, sin contrato: puede
cambiar sin aviso.
