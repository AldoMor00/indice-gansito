# Las fuentes

Qué traen los archivos que se ingestan y qué se puede dar por cierto de ellos. Lo que
**hacemos** al respecto no está aquí: vive en `scripts/ingesta_profeco.py`,
`scripts/ingesta_conasami.py` y `objetivo.yml`.

Las dos salen de `repodatos.atdt.gob.mx`, sin token, y ninguna expone listado de
directorio: la única forma de saber qué hay publicado es pedirlo, o preguntarle al
catálogo (`datos.gob.mx/api/3/action/package_show?id=...`).

---

# Profeco — *Quién es Quién en los Precios*

Medido sobre `01-2024_01` y `11-2025_02` completos, en agosto de 2026.

## Dónde está

```
repodatos.atdt.gob.mx/api_update/profeco/programa_quien_es_quien_precios_AAAA/MM-AAAA_QQ.csv
```

`QQ` es `01` o `02`. Hay 46 quincenas contiguas, de `01-2024_01` a `11-2025_02`, sin
huecos, y nada de 2023.

## Qué trae

15 columnas idénticas entre años, todas texto:

`producto, presentacion, marca, categoria, catalogo, precio, fecha_registro,
cadena_comercial, giro, nombre_comercial, direccion, estado, municipio, latitud, longitud`

Entre 140 y 225 MB y entre 437 y 710 mil filas por archivo, con BOM, CRLF y comas
embebidas entre comillas.

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
  filas, siempre exactamente dos y siempre distintos como número, no como formato. No es
  error de captura: en 169 de 177 al menos uno de los dos valores es un precio que esa
  tienda cobra en su propia serie, y en 26 lo son los dos. Se descomponen en tres fenómenos
  —57 con el precio bajo estable de los dos lados, 35 con el alto estable, que es la
  promoción clásica, y 26 de cambio de precio que las dos capturas cruzan—. Los 17 con firma
  de alza, el bajo empatando con el vecino previo y el alto con el siguiente, la confirman
  **17 de 17** con `previo < siguiente`.
- **Bajo esa clave no cambia nada.** Cadena, giro, estado, municipio y coordenada son
  constantes en las 2,392 tiendas por 46 quincenas, y normalizar —trim, mayúsculas, espacios
  colapsados— no fusiona ni una clave ni deja nulos. Lo que sí rota es el panel: sólo 581
  tiendas (24.3%) aparecen en las 46, con una media de 31.

## El programa dejó de publicar

No es un cambio de ruta ni una caída. Tres cosas apuntan al mismo lado, medidas en agosto
de 2026:

- El patrón de URL sigue sirviendo: `11-2025_02.csv` contesta 206 a un range request, y
  todo lo posterior —diciembre de 2025 y las quincenas de 2026— contesta 503.
- El catálogo arma **un dataset por año**, que es por qué el año va en la ruta.
  `programa_quien_es_quien_precios_2026` no existe: `package_show` responde `Not Found`.
  Los dos que sí existen no se tocan desde diciembre de 2025.
- Profeco sigue publicando en el mismo host: cinco datasets suyos —quejas, telecom,
  comercio electrónico, aerolíneas, buró comercial— se actualizaron en marzo de 2026.

O sea: la ventana de análisis está cerrada en 2024-01 → 2025-11 y no falta nada por
llegar. El cron sigue sondeando por si vuelve, no porque se le espere.

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
