# Índice Gansito

Pipeline de datos de punta a punta en Microsoft Fabric sobre *Quién es Quién en los
Precios* (Profeco): cuánto cuesta un Gansito en México y quién lo vende más caro.

> **Estado:** fase F0 — andamiaje. Todavía no hay datos cargados.

## Arquitectura

```
Profeco (CSV quincenal)
      │  GitHub Actions: descarga, filtra, calcula sha256
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
| **bronze** | ¿Qué dijo Profeco, y cuándo lo dijo? | No castea, no filtra, no deduplica |
| **silver** | ¿Qué es cierto? | Nada pasa a gold sin ser evaluado |
| **gold** | ¿Qué le van a preguntar? | Sólo lo que el modelo semántico consume |

La zona raw vive fuera de Fabric para que la capacidad sea desechable: se puede borrar
el workspace entero y reconstruirlo sin perder un día de historia.

## Estructura

```
.github/workflows/   despliegue a prod con fabric-cicd
fabric/              items de Fabric, sincronizados por git integration
scripts/             lo que corre en GitHub Actions, no en Fabric
docs/                decisiones, diagramas y capturas
```

## Despliegue

Al mergear un PR a `main`, GitHub Actions publica los items de `fabric/` en
`ws-gansito-prod` con [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/).
Prod no está conectado a git: se le despliega, no se le sincroniza.

## Fases

| | | Estado |
|---|---|---|
| **F0** | Andamiaje: repos, workspaces, ramas, despliegue verde | en curso |
| **F1** | Landing: backfill 2024-01 → hoy y cron quincenal | |
| **F2** | Bronze: ingesta idempotente | |
| **F3** | Silver: tipado, MERGE, SCD2, cuarentena, calidad | |
| **F4** | Gold: estrella, Direct Lake y copia pública | |
| **F5** | Documentación y página del portafolio | |

## Decisiones

En [`docs/decisiones.md`](docs/decisiones.md), incluidas las que **no** se harían así
en producción y se hacen así aquí por ser un portafolio.

## Fuente

Profeco, *Quién es Quién en los Precios*, vía `repodatos.atdt.gob.mx`.
CSV quincenales de unos 155 MB cada uno.
