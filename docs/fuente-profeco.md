# La fuente: Profeco, *Quién es Quién en los Precios*

Qué trae el archivo que se ingesta y qué se puede dar por cierto de él. Lo que **hacemos**
al respecto no está aquí: vive en `scripts/ingesta.py` y en `objetivo.yml`.

Medido sobre `01-2024_01` y `11-2025_02` completos, en agosto de 2026.

## Dónde está

```
repodatos.atdt.gob.mx/api_update/profeco/programa_quien_es_quien_precios_AAAA/MM-AAAA_QQ.csv
```

`QQ` es `01` o `02`. Hay 46 quincenas contiguas, de `01-2024_01` a `11-2025_02`, sin
huecos. No existe nada de 2023 ni posterior a noviembre de 2025, y el servidor no expone
listado de directorio: la única forma de saber qué hay publicado es pedirlo.

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
- **`(latitud, longitud, nombre_comercial)` no identifica una tienda.** Dos locales de la
  Central de Abasto comparten la tupla y sólo difieren en `direccion`.
