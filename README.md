# Índice Gansito

Pipeline de datos de punta a punta en Microsoft Fabric sobre *Quién es Quién en los
Precios* (Profeco): cuánto cuesta un Gansito en México y quién lo vende más caro.

> **Estado:** fase F2 — bronze. La zona raw ya tiene las 46 quincenas de 2024 y 2025.

## Arquitectura

```
Profeco (CSV quincenal)   CONASAMI (salario mínimo)
      │                         │  GitHub Actions: descarga, calcula sha256
      └───────────┬─────────────┘  y en Profeco además filtra
                  ▼
indice-gansito-datos  ← zona raw, fuera de Fabric a propósito
                  │
                  ▼
lh_bronze ──▶ lh_silver ──▶ lh_gold ──▶ sm_gansito (Direct Lake) ──▶ rpt_gansito
   crudo       entidades      estrella          │
   + linaje    + SCD2         + agregados       └─▶ CSV público ──▶ Power BI gratuito
```

| Capa | Pregunta que contesta | Regla dura |
|---|---|---|
| **bronze** | ¿Qué dijo la fuente, y cuándo lo dijo? | No castea, no filtra, no deduplica |
| **silver** | ¿Qué es cierto? | Nada pasa a gold sin ser evaluado |
| **gold** | ¿Qué le van a preguntar? | Sólo lo que el modelo semántico consume |

La zona raw vive fuera de Fabric para que la capacidad sea desechable: se puede borrar
el workspace entero y reconstruirlo sin perder un día de historia.

## Estructura

```
.github/workflows/   despliegue a prod, ingesta semanal y CI
fabric/              items de Fabric, sincronizados por git integration
scripts/             lo que corre en GitHub Actions, no en Fabric
tests/               pruebas de scripts/, no de los notebooks
objetivo.yml         qué productos entran al corte de precios
docs/                decisiones, hechos verificados, perfil de las fuentes y capturas
```

Una ingesta por fuente (`ingesta_profeco.py`, `ingesta_conasami.py`) y un notebook de
bronze por fuente, sobre los helpers de `nb_00_config`. Por qué no comparten más que la
descarga, en la decisión #9.

## Despliegue

Al mergear un PR a `main`, GitHub Actions publica los items de `fabric/` en
`ws-gansito-prod` con [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/).
Prod no está conectado a git: se le despliega, no se le sincroniza.

## Fases

| | | Estado |
|---|---|---|
| **F0** | Andamiaje: repos, workspaces, ramas, despliegue verde | hecha |
| **F1** | Landing: backfill 2024-01 → 2025-11, CONASAMI y cron | hecha |
| **F2** | Bronze: ingesta idempotente | en curso |
| **F3** | Silver: tipado, MERGE, SCD2, cuarentena, calidad | |
| **F4** | Gold: estrella, Direct Lake y copia pública | |
| **F5** | Documentación y página del portafolio | |

## Decisiones

En [`docs/decisiones.md`](docs/decisiones.md), incluidas las que **no** se harían así
en producción y se hacen así aquí por ser un portafolio.

## Fuentes

Las dos por `repodatos.atdt.gob.mx`, sin token:

- **Profeco**, *Quién es Quién en los Precios* — CSV quincenales de 140 a 225 MB. El
  programa dejó de publicar en noviembre de 2025, así que la ventana está cerrada en
  46 quincenas.
- **CONASAMI**, salario mínimo — dos CSV de ~20 KB. Contestan cuántos Gansitos compra un
  día de salario, y cuánto del cambio de precio es inflación.

Qué traen y qué no es obvio de ellas, en [`docs/fuentes.md`](docs/fuentes.md).
