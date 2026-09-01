# Hechos verificados

Lo que se midió o se probó una vez, para no volver a investigarlo. Cada punto costó una
prueba y cambia cómo se escribe el código.

No es `decisiones.md`, que dice qué se eligió y por qué: aquí no hay elección, sólo lo que
resultó ser cierto. Lo de las fuentes se mide en `fuentes.md` y aquí se resume en una línea.

## Fabric y OneLake

- **El `SHALLOW CLONE` de Fabric es zero copy de verdad, y sirve para lo que lo queremos**:
  funciona por ruta `abfss` sin lakehouse por defecto, entre lakehouses y **entre
  workspaces**. Medido con las 213,772 filas de precios: `_delta_log` sin un solo parquet
  propio y el origen intacto después de escribirle al clon. Es lo que sostiene la
  decisión #5. `currentWorkspaceName` existe en el context del notebook, así que el guard
  que impide correr una utilidad de dev en prod está probado allá.
- **`Optimize Write` viene prendido por defecto**; V-Order lo decide el *resource profile*
  del workspace, y los nuevos nacen en `writeHeavy`, que lo trae apagado. Lo que rompe un
  clon es `OPTIMIZE` seguido de `VACUUM` —el primero deja huérfanos los archivos que el clon
  referencia y el segundo los borra—, no `VACUUM` solo.
- **Fabric escribe `.platform` sin salto de línea final.** Con uno de más, el item sale
  `uncommitted` al sincronizar aunque el contenido sea idéntico. El `notebook-content.py`
  escrito a mano **sí** sobrevive el round-trip, celda `%run` incluida.
- **OneLake no acepta mezclar GUID y nombre** en `abfss://`: 400
  `FriendlyNameSupportDisabled`. Los dos van por GUID; ver `ruta_tabla` en `nb_00_config`.
- **Spark no habla HTTPS**: los bytes bajan con `requests` al driver. La capacidad **sí**
  sale a `raw.githubusercontent.com`, probado con texto y con binario.
- **`activityId` sirve de id de corrida** y `DESCRIBE HISTORY` ya es la bitácora de
  escrituras. Una tabla de runs propia las duplicaría.

## Las fuentes

- **El estado estable del cron es no hacer nada**, y se sabe por qué: el programa de Profeco
  dejó de publicar, no cambió de ruta. Medido en [`fuentes.md`](fuentes.md).
- **El esquema de la zona raw es estable**: 15 columnas en los 46 parquets de precios, 8 en
  los 46 de tiendas, todas `String`. Por eso bronze escribe con `mergeSchema` apagado.
- **No hay PII** en lo que se persiste, y bronze cabe de sobra en la capacidad.

## CI y despliegue

- **`parameter.yml` se queda vacío**: con el clon de la decisión #5, dev y prod comparten
  rutas y no hay nada que sustituir al desplegar. Hará falta en F4, para las referencias
  entre items, que la git integration guarda como `logicalId` y `fabric-cicd` resuelve al
  publicar.
- **`fab` se instala con `uv tool install --python 3.12`**; con 3.14 truena con pyyaml.
- **Los workflows sólo se registran desde la rama por defecto**: `workflow_dispatch` y
  `schedule` no existen mientras el archivo viva sólo en `dev`.
- **El manifiesto es el estado del pipeline, y `main` es lo que corre.** Un pipeline
  idempotente respecto de su manifiesto deja de serlo cuando el manifiesto cambia de
  dirección: `datos#2` movió la zona raw bajo `profeco/` mientras el script de `main` seguía
  leyendo el de la raíz, así que el cron lo encontró vacío y rehizo cinco quincenas en el
  layout viejo antes de que nadie lo viera ([datos#3](https://github.com/AldoMor00/indice-gansito-datos/pull/3)).
  Mover el estado de una fuente y su productor va junto, y en el mismo merge a `main`.
