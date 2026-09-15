---
name: notebooks-fabric
description: Cómo se escribe un notebook de Spark en indice-gansito — estructura de celdas, el resumen que sale por el exit value del pipeline, cómo escribe cada capa (append, MERGE, replaceWhere) y el formato de notebook-content.py para commitear a git. Úsala siempre que se vaya a escribir, revisar o pegar código de un notebook de Fabric de este proyecto, aunque el pedido suene sólo a "dame el código para dim_tienda", "arma la celda del hecho" o "agrégale esto al notebook".
---

# Notebooks de Fabric en indice-gansito

Las convenciones del repo están en `CLAUDE.md`; el porqué de cada regla, en
`docs/decisiones.md`; lo que la plataforma hace, en `docs/plataforma.md`. Aquí va sólo el
procedimiento para escribir un notebook. Se escribe para que alguien lo lea dentro de seis meses
sin este contexto.

## Antes de escribir

- **Se acuerda el camino por item**, UI o `fab import`, y se pregunta antes de cada escritura
  (`CLAUDE.md`). En los dos casos el commit sale del panel de código fuente del UI.
- **Se baja el item de dev, no se lee de git.** El workspace es lo que corre y git va detrás:
  trabajar sobre la copia de git pisa en silencio lo que se hizo en el UI y no se commiteó.

## Estructura: config, definiciones, corrida

Tres celdas, y una cuarta sólo si separarla aporta algo.

1. **`%run nb_00_config`**, sola. Trae `ruta_tabla`, `CORRIDA`, `DeltaTable`, `F`, las
   compuertas, `clave`, `upsert`, el trío del resumen y el guard que truena si el notebook tiene
   lakehouse por defecto.
2. **Definiciones**: el comentario de cabecera, el perfil de recursos, las constantes y las
   funciones. Aquí no corre nada: se lee de arriba abajo sin pensar en estado.
3. **La corrida**: leer, transformar, escribir, y el `exit` al final.

El comentario de cabecera va en la celda 2, no en una celda de markdown: viaja con el código al
`notebook-content.py`. Dice qué hace el notebook y qué decisiones aplica, por número. Los
comentarios pueden repetir el porqué de una decisión, para no tener que abrir el `.md` desde el
notebook; lo que nunca se omite es el `(decisión #N)`.

El perfil de recursos se declara en cada notebook que escribe, porque con High concurrency la
sesión se comparte y el perfil del anterior seguiría puesto (decisión #33):

| capa | perfil |
|---|---|
| bronze, mantenimiento | `writeHeavy` |
| silver | `readHeavyForSpark` |
| gold | `readHeavyForPBI` |

### La corrida va en tres tiempos: armar, validar, escribir

Todas las escrituras al final, después de todas las compuertas. Spark es perezoso: hasta el
`.write` los DataFrames son la etapa de staging, y una compuerta que truena no deja nada a
medias. Las compuertas se reparten por lo que necesitan ver, y ninguna se repite (decisión #16):

- **de entrada**, sobre el lote crudo: obligatorias vacías (`exige_completo`), un atributo por
  clave natural (`exige_uno_por_clave`), formatos de los que cuelga un regex;
- **de salida**, sobre el DataFrame armado: llaves sin colisión (`exige_llave_unica`);
- **constraint CHECK de Delta** (`exige_invariantes`): predicados de una fila, como `precio > 0`.

Las dimensiones se escriben antes que el hecho: el hecho es el punto de commit del que
`pendientes_*` lee el estado, así que una corrida que muera entre medias deja la quincena
pendiente y no a medias.

## Un solo lugar por donde sale un número

`print()` se queda en el snapshot. Lo que el pipeline recibe es el **exit value**, así que todo
número que importe pasa por el acumulador y sale al final:

```python
RESUMEN = {}


def apunta(paso: str, **datos) -> None:
    """Al log del notebook y al resumen que se devuelve, de una sola escritura."""
    RESUMEN[paso] = datos
    legible = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v}" for k, v in datos.items())
    print(f"{paso:<14}: {legible}")


# ... y como última línea del notebook, porque `exit` corta la ejecución:
notebookutils.notebook.exit(json.dumps({"corrida": CORRIDA, **RESUMEN}, ensure_ascii=False))
```

El pipeline lo lee en `@activity('<notebook>').output.result.exitValue`.

Qué se apunta: **la decisión, no la ejecución**. Cuántas quincenas estaban pendientes, cuántas
filas entraron, qué versión quedó en cada tabla. "Empezó" y "terminó" ya los tiene el monitoring
hub. Las métricas del lote (tasas, deriva, el texto con acento perdido) también van aquí: se
miran, no bloquean.

Un solo `exit`, al final. Con el lote vacío las escrituras de abajo son no-ops por construcción.

## Cómo escribe cada capa

| tabla | patrón |
|---|---|
| bronze | `append`, `mergeSchema` apagado |
| dimensión | `upsert`: `MERGE` por clave, con condición de cambio |
| dimensión SCD2 | `upsert`, con las vigencias ya cerradas en el lote |
| hecho por quincena | `replaceWhere` sobre las quincenas recalculadas, y `exige_clustering` |

Nunca `overwrite` de la tabla completa (decisión #14). Los hechos por quincena llevan liquid
clustering y no partición (decisión #32). Un MERGE que no cambia nada no commitea versión, así que
`upsert` lee sus métricas comparando la versión de antes contra la de después (`plataforma.md`).

## Idempotencia

El backfill y la corrida del cron son el mismo código. Antes de transformar, el notebook calcula
qué falta comparando su capa contra la anterior (`pendientes()` contra el manifiesto en bronze;
`pendientes_silver` y `pendientes_gold` contra el hecho) y trabaja sólo sobre eso. Silver y gold
aceptan `quincenas_pedidas` para forzar quincenas cuando cambian las reglas: como cadena, no como
lista, porque los base parameters sólo llevan escalares (`plataforma.md`).

**El estado estable es no hacer nada, y no es un fallo.** Un notebook que truena porque no había
nada pendiente rompe el cron.

## Tipado y qué truena

ANSI está encendido desde `nb_00_config`, así que **`cast` es "esto tiene que pasar" y
`try_cast` es "esto puede faltar"**, y el `try_cast` lleva comentario diciendo por qué. Dos
resultados, pasa o truena (decisión #15). Un aviso a nivel fila no existe: si siempre vas a ir a
arreglarlo, es un error diferido; si lo publicarías así, es atributo del dato o métrica del lote.

Las llaves son `F.xxhash64` sobre la clave natural, nunca un contador: silver se recalcula sola y
un id que cambia entre corridas rompe a gold. `xxhash64` **salta** los nulos, así que una clave
natural vacía da una llave válida y equivocada, no una nula (`mediciones.md`); por eso lo vacío
se ataja en la entrada y no con un `NOT NULL` sobre el id. Cuando la clave natural puede traer
más de una fila en el lote, se desempata con `row_number()` sobre una ventana ordenada por
período descendente, o el MERGE truena con varias filas de origen bajo la misma clave.

## Que se pueda leer

- **Un paso, una función con nombre.** La celda de la corrida se lee como la lista de lo que hace
  (`ultimo_intento`, `pendientes_silver`, `upsert`), sin rastrear variables intermedias.
- **Los docstrings dicen la razón, no la firma.** Lo que no se deduce leyendo es por qué se
  eligió así.
- **Nombres que dicen qué son**: `lote`, `canasta`, `pendientes`, `vigentes`. Un nombre no se
  reusa para dos cosas en el mismo notebook.
- **Una cadena de Spark se rompe dentro de paréntesis, una operación por línea**, y una línea en
  blanco por idea.
- **Comentarios al lado de la línea que los necesita**: un regex, un `<=>`, un `coalesce` con
  default. Los de bloque explican por qué, no qué, en español y cortos.

## El archivo `notebook-content.py`

El archivo escrito a mano sirve para las dos vías: subirlo con `fab import` o meterlo a git. En
ambas el formato tiene que ser exacto o el item sale `uncommitted` al sincronizar. La plantilla
está en [`assets/plantilla-notebook-content.py`](assets/plantilla-notebook-content.py).

- **`.platform` va sin salto de línea final.** Fabric lo escribe así; con uno de más el item
  aparece como modificado para siempre.
- **El `logicalId` es del item.** Si el `.platform` se escribe a mano, va un GUID nuevo bien
  formado; la git integration le asigna otro al commitear.
- **`--format .py` en los dos sentidos**, o la CLI usa `.ipynb` y el import truena. El mismo
  `import` crea y actualiza.

```bash
fab export ws-gansito-dev.Workspace/nb_20_profeco.Notebook -o <dir> --format .py -f
fab import ws-gansito-dev.Workspace/nb_20_profeco.Notebook -i <dir>/nb_20_profeco.Notebook --format .py -f
```

El `.platform` que baja la CLI trae `logicalId` en ceros: no se copia al repo. El export duplica
los CR de un archivo guardado en CRLF, así que para ver qué cambió se usa el panel del UI y no un
`diff` contra el export (`plataforma.md`).

Renombrar un `nb_NN` después de desplegar deja huérfanos en prod (decisión #4). El nombre se
elige una vez.
