---
name: reporte-y-modelo
description: Cómo se escriben `sm_gansito` y `rpt_gansito` en indice-gansito — dónde viven las medidas y cómo se agrupan, qué admite y qué no admite Direct Lake, los roles del `queryState` por tipo de visual, la retícula de la página, y cómo se sube y se verifica cada cambio con la CLI. Úsala siempre que se vaya a tocar el modelo semántico o el reporte de este proyecto, aunque el pedido suene sólo a "agrega una medida", "arma la página 3", "cámbiale el filtro a ese visual" o "organiza el panel de campos".
---

# El modelo y el reporte en indice-gansito

Las convenciones del repo están en `CLAUDE.md`; el porqué de cada regla, en
`docs/decisiones.md`; lo que Power BI y Direct Lake hacen, en `docs/plataforma.md`. Aquí va sólo
el procedimiento para escribir `sm_gansito` y `rpt_gansito`.

Los dos items se escriben a mano, TMDL el modelo y PBIR el reporte, y se suben con la CLI. El UI
queda para lo que sólo se puede juzgar viéndolo (mover un visual, acomodar un eje) y para el
commit.

## Antes de escribir

- **Se acuerda el camino por item**, UI o `fab import`, y se pregunta antes de cada escritura
  (`CLAUDE.md`). El commit sale del panel de código fuente del UI.
- **Se baja el item de dev, no se lee de git.** Trabajar sobre la copia de git pisa en silencio
  lo que se hizo en el UI y no se commiteó.

## El modelo

### Medidas

Viven todas en `Medidas`, una tabla calculada de una columna oculta, que es lo que la deja hasta
arriba del panel de campos. Los nombres son de presentación (`Índice encadenado`, `Cambio real
%`), con acentos y espacios, porque el nombre de la medida es lo que sale impreso. Las columnas
espejan la fuente en snake_case y se renombran dentro del visual.

- **`formatString` explícito, siempre.** El formato es parte de la medida, no del visual.
- **El docstring `///` dice la razón, no la fórmula.** El DAX ya está debajo.
- **La guarda va antes de la resta.** `BLANK() - 1` es `-1` en DAX (`plataforma.md`): un `0.00%`
  que parece dato es peor que un blanco.
- **Lo que sólo sirve de insumo se oculta**: una guarda compartida, un umbral.

### Las carpetas

Cinco, por dónde se busca una medida al armar una página y no por cómo está escrita. Se ordenan
alfabéticamente en el panel y no se numeran.

| carpeta | qué junta |
|---|---|
| Cobertura y guarda | los conteos que sostienen P5 y hacen visible la guarda |
| Índice | las series que publica una página: nominal, real e INPC |
| Inflación y salario | todo lo que sale de CONASAMI |
| Intervalo | lo que sale de `hechos_ic_indice`, con su propia guarda |
| Nivel de precio | los transversales, que valen con cualquier n |

### Qué admite Direct Lake

- **Columnas calculadas: no.** Lo derivado se calcula en gold y llega como columna real
  (decisiones #21, #24, #30).
- **Tablas calculadas de constantes: sí**, mientras no referencien una tabla Direct Lake. Es la
  salida para un what-if, una tabla de bins y cualquier eje desconectado (decisión #22).
- **Una tabla o columna nueva no se procesa sola**: hasta un refresh, el modelo responde `Cannot
  find table`. `fab api -A powerbi -X post datasets/<id>/refreshes` con `{"type":"Full"}`.

## El reporte

### La retícula

Lienzo de **1920 × 1080**, margen de **36**, ancho útil **1848**. La fila de tarjetas son cuatro
de **444** con **24** de aire (`36, 504, 972, 1440`) y las dos columnas de abajo son de **912**
con el mismo aire. Las páginas nuevas se cuelgan de esas coordenadas para que al pasar de una a
otra nada salte. El slicer de un what-if pide **102** de alto, no los 78 de la fila de tarjetas.

`z` y `tabOrder` van en múltiplos de 100 por visual, en el orden de lectura.

### Los roles del `queryState` van por tipo de visual

| visual | roles |
|---|---|
| `lineChart`, `clusteredBarChart`, `clusteredColumnChart`, `columnChart` | `Category`, `Y` |
| `cardVisual` | `Data` |
| `slicer`, `tableEx` | `Values` |
| `shapeMap` | `Category` (ubicación), `Value` (saturación de color) |
| `azureMap` | `X`, `Y` (longitud y latitud), `Series`, `Size` |

Cada proyección lleva `field`, `queryRef` (`Tabla.Campo`) y `nativeQueryRef`. El nombre del rol
no lo valida el esquema: si se le erra, el visual sube con los pozos vacíos. Cuando no esté en
esta tabla, se pide el visual vacío en el UI y se baja, en vez de adivinar.

**Varias columnas en `Category` son una jerarquía, no un eje compuesto.** Para la serie de
quincenas va `quincena` sola, que ya trae `sortByColumn: orden`.

### Títulos, filtros e interacciones

- **Título con `text` explícito** en `visualContainerObjects.title`: el automático dice "X by Y"
  en inglés.
- **Un filtro de visual sobre una medida** va con `type: "Advanced"` y un `Comparison` con su
  `ComparisonKind` (2 es "mayor o igual") sobre la medida y un literal tipado (`5L`). Sobre una
  columna, `type: "Categorical"` y un `In`. Es lo que sostiene el mínimo de cinco tiendas
  (decisión #23) y el "is not blank" de los slicers (decisión #28).
- **`visualInteractions[].type` es `NoFilter`** cuando un visual sin filtro convive con otros que
  sí lo tienen; `NoEffect` lo rechaza el esquema.
- **La selección por defecto de un slicer** vive en `objects.general[].properties.filter`, con el
  valor entre comillas simples dentro del literal (`"'2025-11_q2'"`).

### Visuales con su maña

- **La banda de confianza** es `objects.error` con `errorRange.explicit`, `isRelative: false` y
  las medidas en `lowerBound`/`upperBound`; `shadeShow: true`, `barShow` y `markerShow` en
  `false`.
- **El dumbbell** es una barra clusterizada con error bars, caps encendidos y la barra apagada con
  `fillTransparency` al 100 (decisión #26).
- **El mapa de México** viaja como recurso estático `mexico.states.topo` referenciado con un
  `ResourcePackageItem`; el tipo de mapa no se escribe a mano.
- **El what-if** con `objects.data.mode` en `'Single'` y `padding.top` en `0D`. Su valor por
  defecto se escribe como `Comparison` con `ComparisonKind: 0` contra un literal `D`, que es la
  forma a la que el UI lo reescribe.

### El tema

Los colores salen de `GansitoMigajon.json`, aplicado encima de Fluent 2. Vive en
`StaticResources/RegisteredResources/` y se declara dos veces en `report.json`: en
`themeCollection.customTheme` con `type: "RegisteredResources"`, y como item `CustomTheme` en un
`resourcePackages` del mismo tipo. Los visuales no llevan colores propios; la excepción es el
degradado del `shapeMap`, en su `dataPoint`. Al pisar un objeto de `visualStyles` se copia
completo del tema base y se cambia sólo el color. `textStyle` de un textbox acepta `fontColor`.

## Subir y verificar

```bash
fab export ws-gansito-dev.Workspace/rpt_gansito.Report -o <dir> -f
fab import ws-gansito-dev.Workspace/rpt_gansito.Report -i <dir>/rpt_gansito.Report -f
```

Tres cosas que cuestan una subida cada una (`plataforma.md`):

- **`fab import` de un `Report` exige `byConnection`**; el `definition.pbir` de git trae `byPath`.
  Se sube lo exportado; el repo conserva `byPath`.
- **`fab import` de un `Report` reemplaza sus recursos estáticos por los de la carpeta.** Antes de
  subir, confirmar que `StaticResources` viene entero.
- **El TMDL se sube con los finales de línea del item**, que son CRLF. Se normaliza el archivo
  entero antes de subirlo.

**El round trip es la verificación.** Después de importar, se vuelve a exportar y se comparan los
JSON normalizados (`json.loads` y `sort_keys`), no byte por byte. Cero diferencias semánticas
quiere decir que aceptó todo.

**Un número se comprueba contra el modelo, no contra el razonamiento.** Toda medida nueva se
corre con `fab api -A powerbi -X post datasets/<id>/executeQueries` antes de darla por buena, y
si reproduce algo ya medido, se compara contra `docs/mediciones.md`. El cuerpo de la consulta va
en ASCII con escapes unicode (`plataforma.md`).
