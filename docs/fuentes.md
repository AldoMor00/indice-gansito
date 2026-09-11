# Las fuentes

Qué traen los archivos que se ingestan y qué se puede dar por cierto de ellos. Lo que
**hacemos** al respecto no está aquí: vive en `scripts/ingesta_profeco.py`,
`scripts/ingesta_conasami.py`, `scripts/ingesta_inpc.py` y `objetivo.yml`.

CONASAMI sale de `repodatos.atdt.gob.mx`, sin token y sin listado de directorio: la única
forma de saber qué hay publicado es pedirlo, o preguntarle al catálogo
(`datos.gob.mx/api/3/action/package_show?id=...`). Profeco salía de ahí también, hasta que
ese canal se congeló y hubo que mudarse a su portal (decisión #34).

---

# Profeco — *Quién es Quién en los Precios*

Medido sobre `01-2024_01` y `11-2025_02` completos, en agosto de 2026. Lo de 2026 se
midió aparte, en septiembre, y va al final.

## Dónde está

Un bundle por año, en el portal de datos abiertos de Profeco:

```
datos.profeco.gob.mx/datos_abiertos/qqp.php            listado
datos.profeco.gob.mx/datos_abiertos/file.php?t=TOKEN   el bundle de un año
```

El `TOKEN` es una cadena de 32 hexadecimales asignada a mano, no derivable: el de 2024
termina en `95c5` y el del diccionario de datos en `95c7`. La única forma de conocer el de
un año es leerlo del listado, que es HTML servido por PHP —sin XHR ni JSON detrás: la
página hace dos peticiones y una es un jQuery que da 404—.

Lo que hay que saber para automatizarlo:

- **Se baja entero o nada.** El bundle ignora `Range` —contesta `200` y `chunked`— y no
  manda `Content-Length`, `Last-Modified` ni `ETag`, con `Cache-Control: no-store`. No hay
  forma barata de preguntarle si cambió. Por eso la sonda es el CSV de metadatos (877
  bytes), que declara `Cobertura temporal` y `Última modificación`.
- **Un token inválido contesta `200` con HTML**, no `404`: una página con un
  `alert('Documento no disponible')`. Validar por código de estado no sirve; hay que ver
  que lo que llegó sea un zip.
- **El zip trae su propio manifiesto.** El directorio central lleva tamaño y CRC32 de cada
  miembro, legibles sin descomprimir. Es lo que permite saber qué quincenas reescribió la
  fuente sin volver a hashear 2.5 GB.
- **No pide nada.** Ni cookie, ni referer, ni `User-Agent`.
- **2025 viene en `.rar`**; 2024 y 2026 en `.zip`. Por eso el script tiene `--local`, que
  procesa los CSV ya extraídos a mano.
- **El nombre del archivo cambió de convención**: `MM-AAAA_01`/`_02` hasta 2025,
  `MM-AAAA_Q1`/`_Q2` desde 2026. El script acepta las dos.

## Es el mismo archivo que servía repodatos

Verificado en septiembre de 2026: las **46** quincenas que el manifiesto había bajado de
`repodatos.atdt.gob.mx` tienen el mismo `sha256` que sus copias dentro de los bundles del
portal. Las 46, byte por byte, incluido el conteo de bytes ya registrado. Y el corte
regenerado desde el portal para `2025-11_q2` sale idéntico al parquet commiteado, tanto
en precios como en tiendas.

O sea que el cambio de canal no partió la serie en dos ni obliga a re-ingestar nada, y el
`url_origen` de esas 46 líneas apunta hoy a un host que da 503 sin que eso invalide su
`sha256`: lo que promete el manifiesto sigue siendo cierto, sólo que por el portal.

## Qué trae

15 columnas idénticas entre años, todas texto:

`producto, presentacion, marca, categoria, catalogo, precio, fecha_registro,
cadena_comercial, giro, nombre_comercial, direccion, estado, municipio, latitud, longitud`

Entre 140 y 225 MB y entre 437 y 710 mil filas por archivo, con BOM, CRLF y comas
embebidas entre comillas.

## Lo que Profeco dice de su método

No hay nota metodológica pública. El portal documenta la cobertura —más de 2,000 productos en
poco más de 1,450 establecimientos de hasta 54 ciudades, actualizado todos los días hábiles— y
describe la muestra como "los establecimientos más destacados de cada ciudad". El criterio de
selección no se publica: no hay marco muestral ni diseño probabilístico declarado.

Y una advertencia explícita de la fuente:

> Los precios sirven exclusivamente como referencia de compra para el consumidor, por la
> metodología utilizada no permiten medir la inflación que se registra en el país.

Profeco añade que "la única autoridad facultada para determinarla es el INEGI". Este proyecto no
publica inflación: publica el cambio de precio de un producto concreto, medido por pareo de
tiendas, y lo deflacta con el INPC que publica INEGI. La advertencia es de la fuente y va
citada, no reinterpretada.

Consultado el 2026-09-09 en <https://www.profeco.gob.mx/precios/quienesquie_nvo.asp>.

## Lo que no es obvio

- **`producto` es el genérico, no el nombre comercial.** El Gansito está en `presentacion`
  (`Paquete con 1 Gansito (50 Gr.)`) y Marinela en `marca`; buscar "Gansito" en `producto`
  no devuelve una sola fila.
- **`fecha_registro` es `yyyy/MM/dd`**, sin ambigüedad de parseo.
- **`S/m` aparece sólo en `marca`**, en un tercio de las filas, y significa "sin marca":
  es granel legítimo —pan dulce, bolillo, cacahuate—, no un centinela de nulo.
- **`(producto, presentacion, marca)` no es estable en el tiempo.** Profeco reclasifica y
  recorta gramajes: las Barritas Marinela pasaron de Pastelillos a Galletas Dulces entre
  2024 y 2025, y el Oreo bajó de 273.6 a 252 Gr. Para silver son productos nuevos, no el
  mismo con otro empaque.
- **La clave de una tienda es `(nombre_comercial, direccion)`, y es la única.** Búsqueda
  exhaustiva de los 255 subconjuntos de los 8 campos sobre las 46 quincenas: ese par no
  colisiona una sola vez, y es el único mínimo que lo logra. `direccion` es la del inmueble
  —Sears y Liverpool comparten la de la plaza, Benavides y Elektra la de la esquina— y
  `nombre_comercial` distingue al inquilino: las 11 direcciones con varios nombres se solapan
  en el tiempo, así que son vecinos y no renombres. Ninguno de los dos alcanza solo.
- **`(latitud, longitud)` no identifica, pero nunca se mueve.** Solas dan 5,352 colisiones
  dentro de quincena, y con `nombre_comercial` 45: dos locales de La Molinera en la Central
  de Abasto de Iztapalapa que sólo `direccion` separa. Profeco geocodifica el mercado, no el
  local —hasta 20 tiendas bajo el punto de Ecatepec, y el 10.1% de las filas cae en una
  coordenada compartida—. A cambio, las 2,392 tiendas conservan su coordenada exacta las 46
  quincenas: es atributo geográfico para gold, no identidad.
- **Una visita no es una quincena.** Las 213,772 filas caen en 126,963 celdas
  tienda-SKU-quincena y sólo 49,411 traen una sola observación: Profeco visita la misma
  tienda hasta cinco veces por quincena. `catalogo` no desempata —separa 82 filas de 86,809—
  y `fecha_registro` es `yyyy/MM/dd` sin hora, así que las visitas de un mismo día no se
  pueden ordenar.
- **177 veces el mismo día, tienda y SKU traen dos precios distintos**, el 0.17% de las
  filas, siempre exactamente dos y siempre distintos como número, no como formato. Lo que sí
  se puede afirmar es que no parecen ruido de captura: en 169 de 177 al menos uno de los dos
  valores es un precio que esa tienda cobra en su propia serie, y en 26 lo son los dos. Qué
  los produce no se sabe. Promoción, cambio de precio a media captura y otras causas
  explicarían partes, pero eso es reconstrucción nuestra y no algo que la fuente diga, así
  que no se tratan como fenómenos clasificados: la fuente es ruidosa y así se trata. No hace
  falta más, porque el grano quincenal los promedia (decisión #12).
- **Bajo esa clave no cambia nada.** Cadena, giro, estado, municipio y coordenada son
  constantes en las 2,392 tiendas por 46 quincenas, y normalizar —trim, mayúsculas, espacios
  colapsados— no fusiona ni una clave ni deja nulos. Lo que sí rota es el panel: sólo 581
  tiendas (24.3%) aparecen en las 46, con una media de 31.

## Lo que cambió en 2026

El programa nunca dejó de publicar: lo que murió fue el canal. Durante nueve meses este
documento dio por cerrada la ventana en `2025-11_q2` porque `repodatos.atdt.gob.mx`
contestaba 503 a todo lo posterior —y lo sigue contestando— y porque el catálogo de
`datos.gob.mx` no tiene un dataset de 2026. Las dos cosas siguen siendo ciertas y las dos
eran la evidencia equivocada: Profeco se mudó a su propio portal y ahí publica **mensual**,
con un mes de rezago. Julio de 2026 salió el 31 de agosto.

Medido en septiembre de 2026 sobre `07-2026_Q2` contra `11-2025_02`:

- **El esquema no se movió.** Las mismas 15 columnas en el mismo orden, con BOM,
  `fecha_registro` en `yyyy/MM/dd` y el corte de quincena en 1–15 / 16–31.
- **El catálogo objetivo está completo.** Los 9 SKUs de `Pastelillos y Pan Dulce
  Empaquetado` que había en la última quincena ingestada siguen los 9, y el corte creció
  de 2,847 filas a 5,946. El Gansito conserva su cadena exacta —`Paquete con 1 Gansito
  (50 Gr.)`, `Marinela`, `Pan`— y pasa de 316 a 664 filas.
- **`catalogo` se acentuó**: `Basicos`→`Básicos`, `Electrodomesticos`→`Electrodomésticos`,
  `Pacic`→`PACIC`. No toca la identidad —ni la de tienda ni la de SKU— pero parte en dos
  cualquier agrupación por ese campo que cruce el año.
- **El panel rota como siempre**: 1,197 de las 1,662 tiendas de `2025-11_q2` siguen en
  `2026-07_q2`, dentro de lo que ya se sabía del panel.
- Lo estacional se comporta: sale `Navideños`, entran `Útiles Escolares` y `Tenis`.

Lo anterior a 2026 no se volvió a medir: las afirmaciones de arriba siguen acotadas a las
46 quincenas sobre las que se hicieron.

## El archivo no siempre trae las mismas columnas

**Junio de 2026 salió con 18 columnas en vez de 15**, sus dos quincenas, y julio volvió a
15. Las tres de más son las llaves internas de Profeco:

- **`folio` es la tienda.** 411 folios para 411 tiendas en `2026-06_q1` y 388 para 388 en
  `2026-06_q2`, uno a uno en las dos direcciones. Confirma, contra el id propio de la
  fuente, que la clave `(nombre_comercial, direccion)` deducida arriba era la correcta.
- **`cv_marca` es el SKU, no la marca.** Nueve valores para los nueve SKUs del corte
  objetivo: Marinela sale como 1, 2 y 4, y Bimbo como 10, 25, 29, 32, 33 y 34.
- **`cv_producto` es el producto genérico.** Un solo valor en el corte, porque el corte es
  un solo `producto`.

No es una migración de esquema ni algo que se pueda esperar: es el sistema interno
asomándose un mes. Por eso `corte_precios` declara sus 15 columnas y tira lo demás, igual
que `corte_tiendas` con las suyas —era la asimetría que dejó pasar esto—, y por eso bronze
escribe con `mergeSchema` apagado: la corrida que tumbó junio hizo exactamente lo que la
decisión #8 le pide.

## Ni la misma codificación

**Mayo de 2026 llegó en cp1252 y sin BOM**, sus dos quincenas; las otras 60 vienen en utf-8
con BOM. Comprobado archivo por archivo sobre los 62: son los únicos dos que no decodifican
como utf-8, y también los únicos dos sin BOM.

Eso no truena solo, y ahí está el problema: leer cp1252 como utf-8 *lossy* cambia cada byte
inválido por `U+FFFD` en vez de fallar, así que `Panqué` se vuelve `Panqu?` y entra al repo
como si nada —**15,125 celdas** entre las dos quincenas—. Lo que sí truena es la compuerta
de silver, tres capas después y disfrazado: `Panqué Nuez` y `Panqu?  Nuez` cuentan como dos
presentaciones distintas, así que la canasta de 9 pasó a 11 y `nb_20` detuvo la corrida.

La ingesta comprueba la codificación antes de leer y se cae a cp1252 cuando hace falta
(decisión #37). El síntoma vale como aviso general: **una fuente puede corromperse sin que
nada falle**, y lo único que lo cazó fue una regla de calidad que contaba SKUs.

---

# CONASAMI — salario mínimo

Medido en agosto de 2026 sobre los cuatro CSV del catálogo.

## Dónde está

```
repodatos.atdt.gob.mx/api_update/conasami/salarios_minimos/NOMBRE.csv
```

De los cuatro archivos se ingestan dos. `sm_real_indice` trae el nominal mensual y el
deflactor; `sm_general_profesionales_zonas` trae el salario vigente por zona. Los otros
—`sm_historico_anual` y `sm_general_profesionales_capital`— no responden ninguna pregunta
del proyecto.

## Qué traen

**`sm_real_indice.csv`** — 685 filas × 5 columnas, 18 KB. Una fila por mes, de `1969-01`
a `2026-01`.

`anio, mes, smg_nominal, smg_real, smgr_indice`

**`sm_general_profesionales_zonas.csv`** — 42 filas × 89 columnas, 20 KB. Una fila por
`(inicio_vigencia, zona_salarial)`, de 2009 a 2026.

`inicio_vigencia, zona_salarial, salario_minimo_general`, y 86 columnas más: un salario
profesional por oficio, de `albanileria` a `zapatero`.

## Lo que no es obvio

- **La serie mensual crece en tandas anuales.** El catálogo la declara `Anual` y así se
  comporta: llega hasta `2026-01` y el paquete se modificó en marzo de 2026. Publican la
  serie completa una vez al año, cuando entra el salario nuevo en enero.
- **Aun así cubre la ventana de precios completa.** Los 23 meses de `2024-01` a `2025-11`
  están todos, y la serie llega dos meses más allá del último dato de Profeco.
- **El deflactor viene dentro:** `smg_nominal / smg_real` es el INPC entre 100, verificado
  contra el 133.554 que INEGI publicó para `2024-01`.
- **Ese deflactor es mensual**, así que las dos quincenas de un mes comparten el suyo. El
  INPC quincenal existe y sería lo correcto, pero sólo sale por la API de indicadores de
  INEGI, con token de registro.
- **Hay tres cifras de salario para 2025 y no se contradicen**: son tres conceptos en tres
  archivos.

  | valor | archivo | qué es |
  |---|---|---|
  | 278.80 | `..._zonas` | salario **por zona**, `general`, vigente desde 2025-01-01 |
  | 289.75 | `sm_real_indice` | ponderado nacional **mensual**, igual los 12 meses de 2025 |
  | 289.68 | `sm_historico_anual` | ponderado nacional **anual** |

  Las dos últimas sí son el mismo concepto por métodos distintos: difieren en 7 centavos.
  `smg_nominal` es nacional ponderado; el salario que de verdad se paga en una zona vive
  sólo en `..._zonas`.
- **`zona_salarial` se renombró en 2025.** Dice `resto del pais` en 2023 y 2024, y
  `general` en 2025 y 2026. Es la misma zona —la no fronteriza—, no una categoría nueva.
  La otra, `zlfn`, no cambia de nombre.
- **Cruzar la zona contra el precio exige un catálogo que no está aquí.** La Zona Libre de
  la Frontera Norte se define por municipio, y el archivo no los lista; para pegarla contra
  el `estado` y `municipio` de Profeco haría falta el padrón de municipios fronterizos.

---

# INEGI — INPC quincenal

Medido en septiembre de 2026.

## Dónde está

La API de indicadores, que pide token:

```
inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/910420/es/00/false/BIE-BISE/2.0/{token}?type=json
```

`910420` es el **nivel del índice** del INPC general, base 2018=100 y grano **quincenal**.
928 observaciones, de `1988/01/01` a `2026/08/02`, y la respuesta pesa 140 KB.

## Lo que costó encontrarla

Nada de esto está en la documentación de INEGI, que sólo trae ejemplos de población, y
los tres errores dan **el mismo** `400` con "No se encontraron resultados" —indistinguible
de un token inválido—:

- La fuente es **`BIE-BISE`**, no `BIE`.
- La geografía nacional es **`00`**, no `0700`, que aparece en ejemplos viejos.
- El id no se puede deducir. Salió de recorrer el catálogo del navegador de indicadores.
  Se reconoce por tema `189128` ("Índice"), unidad `1051` ("Índice base 2018=100") y
  frecuencia `15` ("Quincenal"), y casi todos sus vecinos de id son variación porcentual
  en vez de nivel: `910445`, por ejemplo, es la inflación quincenal anual.

`false` en la penúltima posición pide la serie completa; con `true` la API devuelve sólo
la última observación.

## Lo que no es obvio

- **El mensual es el promedio de sus dos quincenas.** Enero de 2024 son 133.34 y 133.77,
  que promedian 133.5550 contra el 133.554 que el proyecto ya tenía medido desde CONASAMI
  (`hechos.md`). Es la comprobación de que cambiar de fuente el deflactor no mueve la
  cadena: una milésima de redondeo.
- **La respuesta es estable byte a byte** entre llamadas seguidas: no trae marca de tiempo
  de la petición. Por eso el `sha256` sirve para decidir si hay algo que escribir.
- **`LASTUPDATE` viene dentro de la serie** y dice cuándo la actualizó INEGI, que no es lo
  mismo que hasta cuándo llega. Las dos cosas se guardan en el manifiesto.
- **La descarga sin token existe y no sirve.** El CSV de datos abiertos del programa INPC
  —`inpc_indicador_mensual_csv.zip`— se baja sin credencial, pero su última observación es
  de **julio de 2024**. Es la razón por la que se paga el costo del token.
- **El token no entra al repo.** Va en la URL, así que el manifiesto guarda `{token}` en
  su lugar. Vive como secreto `INEGI_TOKEN` del repositorio.

---

# Profeco — la API del portal, que no se usa

Medido en septiembre de 2026. **Está aquí porque se investigó y se descartó**, no porque
se ingeste: ver la decisión #36.

El portal de consulta —`qqp.profeco.gob.mx`— no es una página estática sobre los mismos
archivos: tiene su propia API JSON, sin token, sin cookie y sin referer. Las rutas salen
del bundle de la aplicación; ninguna está documentada.

| ruta | qué devuelve |
|---|---|
| `/api/catalogo` | los 9 catálogos: `bas`, `ele`, `fru`, `jug`, `med`, `nav`, `pes`, `uti`, `esp` |
| `/api/productos/{clave}` | 12,178 productos con `CVE_PRODUCTO`, `CVE_MARCA`, `MARCA`, `PRESENTACION`, `DES_PRODUCTO` |
| `/api/establecimientos` | 7,607 filas con `folio`, `establecimiento`, `cadena_comercial`, `entidad` |
| `/api/catalogo/entidades`, `/api/ciudades` | claves de estado y de ciudad |
| `/api/producto?tipo=...` | precios **del día**, con dirección, colonia y CP |

## Son las mismas llaves que se filtraron en junio

El Gansito en el catálogo es `CVE_PRODUCTO='0008'` y `CVE_MARCA='002'`; lo que trajo junio
fue `cv_producto='8'` y `cv_marca='2'`. La misma llave sin los ceros a la izquierda, y los
nueve `cv_marca` de junio son uno a uno los `CVE_MARCA` del catálogo para el producto 0008.

## Por qué no resuelve nada aquí

- **No mete las llaves en la historia.** Los archivos masivos, que son los que tienen las
  62 quincenas, siguen sin traerlas. Enlazar el catálogo con ellos exige unir por texto, y
  ahí ya hay fricción: de los 9 SKUs del corte, 8 empatan exacto y el noveno no —el
  catálogo dice `PAQUETE C/8. DONITAS. ESPOLVOREADAS (140 GR.)` y el archivo dice
  `Paquete con 8 Donitas. Espolvoreadas (140 Gr.)`, y son el mismo `0008/029`.
- **`/establecimientos` no sustituye a `dim_tienda`.** Trae 3,037 folios en 7,607 filas
  —hay folios repetidos hasta 897 veces—, no incluye dirección, y es el padrón *actual*:
  sólo 328 de los 388 folios de junio aparecen ahí. Las tiendas que salieron del panel no
  están.
- **El endpoint de precios es más fresco y no sirve para la serie.** Devuelve
  observaciones del día —`fecha_observacion` de hace dos días, contra el mes de rezago de
  los archivos— pero sin llaves y sin historia.

Y una advertencia que vale para todo lo de arriba: es la API interna del portal, sin
documentar y sin contrato. Puede cambiar sin aviso, a diferencia de los archivos masivos.
