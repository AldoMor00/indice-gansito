# Mediciones

Lo que dan los datos y el índice, medido una vez para no volver a medirlo. Cada sección dice
sobre qué ventana y cuándo. Lo que la plataforma hace está en [`plataforma.md`](plataforma.md);
lo que son los archivos de origen, en [`fuentes.md`](fuentes.md); lo que se eligió a partir de
esto, en [`decisiones.md`](decisiones.md).

**La ventana vigente son 62 quincenas**, de `2024-01_q1` a `2026-07_q2`, medidas en septiembre
de 2026 con `executeQueries` sobre `sm_gansito`. Las cifras son **provisionales**: la serie tiene
cuatro quincenas después del escalón de $15.00 y seguían entrando tiendas en la última.

Las secciones marcadas **(46)** se midieron sobre las primeras 46 quincenas, hasta `2025-11_q2`,
y no se han vuelto a medir. Se conservan porque sostienen una decisión.

## Los datos que entran

Sobre las 62 quincenas, verificado por las compuertas de `nb_20` y `nb_21`.

- **Todo castea.** `precio` a `decimal(10,2)`, la coordenada a `decimal(9,6)`, el gramaje a
  `decimal(7,2)`, con ANSI prendido y sin un fallo. Las dos tablas de CONASAMI también. El dato
  que no castea nunca ha existido en estas fuentes (decisión #15).
- **Y cuando se inyecta, truena.** Un `precio = "N/D"` metido al lote sale como
  `CAST_INVALID_INPUT` en la agregación del hecho, no como nulo, y `hechos_precios` no avanza de
  versión. Es el único camino de fail fast que la fuente no ejerce sola; está en `nb_90_pruebas`.
- **Ninguna columna que silver necesita viene vacía**, ni nula ni cadena vacía, salvo la
  coordenada desde `2026-04_q2` (decisión #38). Es lo que deja hashear las llaves sin riesgo.
- **La llave válida y equivocada tiene número.** `clave("nombre_comercial", "direccion")` da
  `8554209004007291361` tanto para `("Oxxo", null)` como para `(null, "Oxxo")`: dos claves
  naturales, una sola llave, ninguna nula. Por eso lo vacío se ataja en la entrada y no con
  `NOT NULL` sobre el id (decisión #16). Probado en `nb_90_pruebas`, que saca un ERROR rojo por
  cada prueba que **pasa** (Spark loguea la excepción aunque se capture): lo que dice si algo
  falló es el exit value.
- **La identidad de tienda, sin normalizar, se parte: 4,484 llaves crudas donde hay 2,956
  canónicas.** Ninguna de las partidas viene de las primeras 46 quincenas; todas del lote que
  empieza en `2025-12_q1`, aunque la mayoría de las tiendas afectadas ya existían antes con una
  sola grafía. Normalizadas (decisión #39), ningún atributo cambia bajo la clave: los conflictos
  crudos de cadena, giro, estado y municipio son todos grafías, y vuelven a cero.
- **`xxhash64` no colisiona** en las tiendas ni en los nueve SKUs, y las nueve presentaciones
  casan con los dos formatos conocidos. Reconstruir una dimensión no mueve el índice: las llaves
  son deterministas, así que los hechos vuelven a empatar con las mismas filas.
- **El tabulador de CONASAMI no deja huecos.** Sus 42 filas son 21 `inicio_vigencia` y 6 zonas
  de 7 literales; las tres salidas son fusiones y ninguna zona vuelve. Por eso el cierre de
  vigencias puede ser global (decisión #17), y la compuerta de `nb_21` truena si una reaparece.
- **El INPC de INEGI empata con el deflactor que traía CONASAMI**: el promedio de las dos
  quincenas de enero de 2024 da 133.5550 contra 133.554. Cambiar de fuente el deflactor no mueve
  la cadena (decisión #35).
- **No hay PII** en lo que se persiste.

## El método del índice

### Por qué encadenado, y por qué Jevons **(46)**

Medido sobre la zona raw con el mismo grano y las mismas llaves que `nb_20`, para el SKU
`Paquete con 1 Gansito (50 Gr.)`.

- **El panel rota mucho más de lo que sugiere el agregado.** Sólo el 6 % de las tiendas que
  vendieron Gansito aparece en todas las quincenas, pero eslabón a eslabón el traslape es alto:
  la mediana del pareo retiene el **86 %** del n disponible. La rotación asusta en el agregado y
  es inofensiva entre quincenas vecinas.
- **El encadenado suelta tiendas y no geografía.** Mediana por eslabón: conserva el 86 % de las
  tiendas, el 95 % de las cadenas y de los municipios, y el 100 % de los estados. Es lo que deja
  leer P2 y P3 sobre la misma muestra.
- **Las cuatro variantes, con bootstrap de 2,000 réplicas sobre tiendas:**

  | variante | cambio | EE | IC 95 % | n que usa |
  |---|---|---|---|---|
  | promedio simple | +25.63 % | 1.11 | 23.48 – 27.76 | 100 % |
  | panel balanceado | +31.75 % | 3.09 | 25.91 – 38.03 | 9 % |
  | pareo en las puntas | +29.55 % | | | 46 % |
  | Jevons encadenado | +24.40 % | 1.60 | 21.36 – 27.64 | 86 % |

  El panel balanceado es lo peor de los dos mundos: el más sesgado **y** el más impreciso. Su
  cifra alta es sesgo de supervivencia. El sesgo de composición del promedio simple es chico,
  1.2 puntos, pero en dirección desconocida.
- **Parear cuesta precisión, pero poco, por dos razones medibles.** El pareo cancela la
  dispersión que no importa (la desviación de log-precios *entre* tiendas es el doble que la del
  *cambio dentro* de una tienda), y las mismas tiendas reaparecen eslabón tras eslabón, así que
  sus errores se telescopan: sumar varianzas en cuadratura da 2.62 pp y el bootstrap 1.60. Es la
  razón de que el intervalo no se pueda calcular en DAX (decisión #20).
- **Carli sobreestima 12 puntos**: +37.06 % contra +24.40 % de Jevons y +23.90 % de Dutot. Dos
  tiendas que intercambian precios promedian +25 % en Carli cuando en conjunto no pasó nada.
- **El intervalo de la canasta no sirve como intervalo del Gansito.** Tabulado sin separar SKU
  sale cuatro veces más angosto, porque promedia nueve series. De ahí el grano SKU × quincena
  (decisión #20).
- **Gold reproduce la exploración.** El bootstrap de `nb_30_gold`, con otra semilla, da un EE de
  1.6027 pp contra 1.60 y el mismo intervalo a una décima: ruido de Monte Carlo.
- **El bootstrap remuestrea tiendas**, pero el diseño de Profeco no es aleatorio: eligen a quién
  visitar y no publican el criterio (`fuentes.md`). El intervalo aproxima la variabilidad; no es
  inferencia sobre todas las tiendas de México. Va impreso junto al número.

### La guarda **(62)**

- **El umbral decide la granularidad del corte, no el gusto.** Con el `giro` crudo de Profeco
  sólo una de cinco categorías junta 30 tiendas pareadas; colapsado a tres canales pasan dos, y
  tradicional no (decisión #19).
- **Un solo eslabón flaco apagaba una serie entera con la regla vieja.** Con 62 quincenas
  conveniencia pareaba 29 en `2025-12_q1` contra un umbral de 30, con un pareo mediano de 50: un
  eslabón flaco, no muestra insuficiente. `Cambio encadenado %` se iba en blanco mientras
  `Índice encadenado` seguía dibujando. Es lo que destapó la decisión #42.
- **Excluir los eslabones de menos de 30 sesga en vez de limpiar.** Omitir uno equivale a
  asignarle cambio cero, y en cortes chicos los flacos no caen al azar: en Wal-mart los 15
  excluidos coinciden con una bajada del Gansito de −11.13 % en todos los canales, y su cambio
  pasa de +10.04 % a +22.82 %.
- **Con el pareo efectivo publican** conveniencia (46.3), supermercado (194.2), Hipermercado
  Soriana (32.2), Wal-mart (31.7) y Ciudad de México (44.0). No publican tradicional (17.0),
  Bodega Aurrera (24.7), Chedraui (19.9) ni Oxxo (17.3). Los nueve SKUs van de 230 a 301. Ninguna
  serie reaparece después de apagarse.
- **El pareo efectivo sigue al error del bootstrap sólo a medias.** Repitiendo el bootstrap por
  corte en local, la correlación de rangos entre el error estándar y la suma de 1/n es 0.71 en
  40 cortes. No ve la dispersión de precios: Oxxo, con pareo efectivo de 17, tiene error de
  3.02 pp, y Wal-mart, con 32, de 5.58. Sumar varianzas en cuadratura sigue mejor al bootstrap
  (0.82) pero sobreestima el doble. Es el límite escrito en la decisión #42.

## Cifras vigentes **(62)**

### El índice

- **El Gansito llegó a 124.40 en `2025-11_q2` y cierra en 106.94 en `2026-07_q2`**: +6.94 %
  nominal y −1.83 % real contra +8.93 % del INPC quincenal en el mismo tramo. Casi toda la caída
  está en tres eslabones: `2026-05_q2` −5.93 %, `2026-06_q1` −8.63 % y `2026-06_q2` −2.22 %. Los
  otros ocho SKUs se mueven ±1 % en esas mismas quincenas.
- **En junio de 2026 el autoservicio bajó el Gansito a $15.00, y es precio, no defecto.** Ninguna
  tienda marcaba $15.00 en `2026-05_q1`; en `2026-07_q2` son 133 de 332 (40 %). La cohorte de 227
  tiendas que tocan $15.00 sigue la trayectoria del resto del panel durante dos años y medio
  antes de separarse; venía de $22 o $23, así que el recorte es de un tercio. Es del canal moderno
  y no de una región: el 54 % de supermercado, el 6 % de conveniencia y el 0 % de tradicional
  (Chedraui 91 %, Soriana 72 %, Wal-mart 56 %, Bodega Aurrera 53 %; Oxxo, farmacias y mercados en
  0 %), en 26 de los 27 estados con al menos cinco tiendas. Los descuentos intermedios que
  aparecen son visitas mezcladas dentro de la quincena: `precio_promedio` promedia, y
  18.50 = (22 + 15) / 2.

  Se descartaron las tres lecturas de defecto. No es otro producto: hay una sola presentación
  con "Gansito" en las 62 quincenas. No es la identidad de tienda: en las 75 tiendas donde el
  Gansito cae más de 10 % en `2026-06_q1`, los otros ocho SKUs tienen mediana 0.0 %. No es el
  archivo de junio (decisión #36): pegaría a los nueve SKUs. Fuera de los datos, hay
  supermercados anunciando el Gansito a $15 y en farmacia sigue arriba de $20.

  Lo que queda abierto es si dura: cuatro quincenas después del escalón, con tiendas todavía
  entrando. Por eso las cifras son provisionales.
- **Por corte, cambio del periodo con el pareo efectivo:** conveniencia +9.65 %, supermercado
  +4.65 %, Hipermercado Soriana +0.74 %, Wal-mart +10.04 %, Ciudad de México +12.98 %.
- **Con 62 quincenas la serie de conveniencia acumula también el eslabón que no dibuja** y
  cierra en 109.65.

### El salario **(50 quincenas)**

La página del salario llega hasta `2026-01_q2`, donde termina la serie mensual de CONASAMI
(decisión #40).

- **Un día de salario mínimo pasa de 15.6 a 15.4 Gansitos.** El Gansito sube +24.86 % en ese
  tramo y el salario +25.48 %: el escalón de enero de 2026 casi empata el precio acumulado.
- **El salario es escalón, no serie.** CONASAMI lo fija una vez al año: `Índice salario mínimo`
  es plano las 24 quincenas de 2024, salta en `2025-01_q1` y otra vez en `2026-01_q1`. Contra un
  precio que se mueve todo el año, los Gansitos por día bajan hasta un piso de 13.1 en
  `2024-11_q2`, rebotan con cada escalón y vuelven a bajar.
- **Las dos escalas de P4 no empatan, y la diferencia es el pareo** (decisión #29): la lectura
  por nivel sale de `Precio promedio`, transversal, y la lectura por serie del encadenado.

### El corte transversal

Medido sobre `2025-11_q2` cuando la serie tenía 46 quincenas; con 62, P2 arranca en
`2026-07_q2` y no se ha vuelto a medir. Se conserva lo que sostiene una decisión:

- **El precio es de anaquel y no continuo**: pocos precios distintos en un rango de diez pesos, y
  uno domina con más de un cuarto de las tiendas. Es lo que pide bins de un peso con el borde en
  el medio peso (decisión #22).
- **La mayoría de las cadenas trae de una a tres tiendas.** Sin un mínimo, la cadena más cara es
  una farmacia con dos tiendas (decisión #23).
- **Los nombres de municipio se repiten entre estados.** "Benito Juárez" junta el de Ciudad de
  México con el de Quintana Roo: agregados por nombre dan una fila a un precio que no existe en
  ningún lado (decisiones #23, #30).
- **`canal` es nulo para más de la mitad del padrón**: las tiendas de giros que no venden
  pastelillos, y ninguna tiene una celda en `hechos_precios`. Es lo que obligó a la decisión #27.
- **Colima y Nayarit no aparecen nunca** en el padrón: el mapa los deja sin pintar con razón
  (decisión #24).

## La copia pública

Medido sobre el export de las primeras 46 quincenas, que es el que está en disco y sin
commitear; el de 62 no se ha hecho.

- **Gold entero pesa menos de un megabyte, en ocho parquets planos.** `hechos_precios` y
  `hechos_relativos` son dos tercios; las dimensiones pesan kilobytes. Soltar la partición y
  prender el V-Order redujo los dos hechos a la mitad y dejó las otras seis idénticas al byte.
- **El `coalesce(1)` de `nb_50_export` ya no argumenta compresión**: la tabla ya viene en un
  archivo. Sigue haciendo falta para que la URL que consume Power BI tenga nombre fijo y no un
  `uuid` por corrida.
- **El repo de datos queda muy por debajo del techo blando de 1 GB** que GitHub recomienda, y el
  parquet más grande está lejos de los 50 MiB por archivo donde avisa. Git guarda cada versión y
  el parquet no hace delta, así que cada reexport suma alrededor de un megabyte.
- **Los parquets reproducen el índice sin Fabric de por medio.** Leyéndolos con polars y sumando
  los log-relativos por quincena, el cambio encadenado del Gansito da la misma cifra que publica
  el modelo. Traen todas las columnas, incluida la `_quincena` oculta, así que el TMDL del modelo
  import se copia sin renombrar nada.
