# Índice Gansito

Pipeline de datos de punta a punta en Microsoft Fabric sobre *Quién es Quién en los Precios*
(Profeco): cuánto cuesta un Gansito en México, quién lo vende más caro, y cuánto de lo que sube es
inflación.

**Estado:** en producción desde 2026-09-14. Corre solo: la ingesta el lunes, el pipeline el martes,
el mantenimiento el domingo. Serie de 62 quincenas, de `2024-01_q1` a `2026-07_q2`.

## Qué dice hoy

Medido en septiembre de 2026 sobre las 62 quincenas. Las cifras son **provisionales**: en junio de
2026 el autoservicio bajó el Gansito a $15.00 y la serie sólo tiene cuatro quincenas después de
ese escalón (`docs/mediciones.md`).

| | |
|---|---|
| Gansito, cambio encadenado `2024-01_q1` → `2026-07_q2` | **+6.94 %** nominal, **−1.83 %** real |
| INPC en el mismo tramo | +8.93 % |
| Máximo de la serie, `2025-11_q2` | 124.40 (base 100) |
| Un día de salario mínimo, `2024-01_q1` → `2026-01_q2` | 15.6 → 15.4 Gansitos |

El índice parea cada quincena contra la anterior sobre las tiendas presentes en las dos y encadena
los eslabones (decisión #19). Publica en blanco antes que publicar de más: si un corte no junta
el pareo mínimo, no hay cifra (decisión #42).

## Arquitectura

```
Profeco (CSV quincenal)   CONASAMI (salario mínimo)   INEGI (INPC quincenal)
      │                         │                          │
      └─────────────────────────┼──────────────────────────┘
                                ▼   GitHub Actions: descarga, sha256; en Profeco además corta
                     indice-gansito-datos   ← zona raw, fuera de Fabric a propósito
                                │
                                ▼
lh_bronze ──▶ lh_silver ──▶ lh_gold ──▶ sm_gansito (Direct Lake) ──▶ rpt_gansito
   crudo       identidad      estrella          │
   + linaje    + calidad      + intervalo       └─▶ parquet público ──▶ PBIX import (pendiente)
```

| Capa | Pregunta que contesta | Regla dura |
|---|---|---|
| **bronze** | ¿Qué dijo la fuente, y cuándo? | No castea, no filtra, no deduplica |
| **silver** | ¿Qué es cierto? | Nada aterriza si una compuerta truena |
| **gold** | ¿Qué le van a preguntar? | Sólo lo que el modelo consume |

La zona raw vive fuera de Fabric para que la capacidad sea desechable: se puede borrar el workspace
entero y reconstruirlo sin perder un día de historia (decisión #1).

## Cómo corre

- **Lunes**, GitHub Actions (`ingesta.yml`): baja lo nuevo de las tres fuentes y lo commitea en
  `indice-gansito-datos`. El estado estable es no encontrar nada.
- **Martes 03:00**, `pl_master` en prod: `pl_bronze` → `pl_silver` → `pl_gold`, y al final el
  refresh del modelo.
- **Domingo 03:00**, `pl_mantenimiento` en prod: `OPTIMIZE`, `VACUUM`, refresh, y re-clon del
  bronze de dev desde prod.

`ws-gansito-dev` está sincronizado por git integration con la rama `dev`. `ws-gansito-prod` no está
conectado a git: al mergear un PR a `main`, GitHub Actions le despliega los items de `fabric/` con
[`fabric-cicd`](https://microsoft.github.io/fabric-cicd/). Lo que los dos workspaces necesitan
configurado a mano está en [`docs/entorno.md`](docs/entorno.md).

## Estructura

```
.github/workflows/   ingesta semanal, despliegue a prod y CI
.claude/skills/      cómo se escribe un notebook, y cómo el modelo y el reporte
fabric/              items de Fabric, escritos por la git integration
scripts/             lo que corre en GitHub Actions, no en Fabric
tests/               pruebas de scripts/; los notebooks se prueban en nb_90_pruebas
objetivo.yml         qué productos entran al corte de precios
docs/                decisiones, plataforma, mediciones, fuentes, entorno, capturas
```

## Mapa de lectura

| quiero | leo |
|---|---|
| entender por qué está hecho así | [`docs/decisiones.md`](docs/decisiones.md): las líneas **Decisión** dan la vista completa; el motivo y el costo van debajo |
| escribir un notebook, o tocar el modelo o el reporte | [`CLAUDE.md`](CLAUDE.md), la skill del item, y [`docs/plataforma.md`](docs/plataforma.md) |
| comprobar un número del reporte | [`docs/mediciones.md`](docs/mediciones.md) |
| saber si un dato raro es de la fuente | [`docs/fuentes.md`](docs/fuentes.md) |
| reconstruir un workspace desde cero | [`docs/entorno.md`](docs/entorno.md) |

Las decisiones incluyen las que **no** se harían así en producción y se hacen así aquí por ser un
portafolio. Se dice en cada una.
