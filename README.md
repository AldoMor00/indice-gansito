# Índice Gansito

Cuánto cuesta un Gansito en México, quién lo vende más caro, y por qué subió más
que sus hermanos. Pipeline de datos de punta a punta en Microsoft Fabric sobre
*Quién es Quién en los Precios* de Profeco.

> **Estado:** fase F0 — andamiaje. Todavía no hay datos cargados.

---

## El hallazgo que originó el proyecto

Entre enero de 2024 y enero de 2025, el precio promedio del Gansito de 50 g subió
**+15.1%** (de $17.09 a $19.66), mientras que los ocho pastelillos hermanos de la
misma marca se movieron entre **+1.0% y +1.9%**. Mismo gramaje, mismo empaque, misma
tupla de producto en la fuente.

En un mismo mes el mismo producto se vendió entre **$14.00 y $24.50** — una dispersión
del 75%. Y cuatro cadenas que compiten entre sí (Walmart, Bodega Aurrera, Oxxo y
La Comer) lo tenían en exactamente **$20.00**.

> ⚠️ Estas cifras son un hallazgo preliminar de la fase de exploración y **falta
> descartar** que el +15.1% venga de un cambio en la mezcla de tiendas de la muestra
> y no de un movimiento real de precio. Validarlo es parte de la fase F4.

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

La zona raw vive **fuera** de Fabric para que la capacidad sea desechable: se puede
borrar el workspace entero y reconstruirlo sin perder un solo día de historia.

## Capas

| Capa | Pregunta que contesta | Regla dura |
|---|---|---|
| **bronze** | ¿Qué dijo Profeco, y cuándo lo dijo? | No castea, no filtra, no deduplica |
| **silver** | ¿Qué es cierto? | Nada pasa a gold sin ser evaluado |
| **gold** | ¿Qué le van a preguntar? | Sólo lo que el modelo semántico consume |

## Estructura

```
.github/workflows/   despliegue a prod con fabric-cicd
fabric/              items de Fabric, sincronizados por git integration
scripts/             lo que corre en GitHub Actions, no en Fabric
docs/                decisiones, diagramas y capturas
```

## Despliegue

Al mergear un PR a `main`, GitHub Actions despliega los items de `fabric/` a
`ws-gansito-prod` con [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/).
Prod no está conectado a git: se le despliega, no se le sincroniza.

Python se maneja **siempre** con [uv](https://docs.astral.sh/uv/).

## Fases

| | | Estado |
|---|---|---|
| **F0** | Andamiaje: repos, workspaces, ramas, CI y despliegue verdes | en curso |
| **F1** | Landing: backfill 2024-01 → hoy y cron quincenal | |
| **F2** | Bronze: ingesta idempotente | |
| **F3** | Silver: tipado, MERGE, SCD2, cuarentena, calidad | |
| **F4** | Gold: estrella, Direct Lake y copia pública | |
| **F5** | Documentación y página del portafolio | |

## Decisiones

Las decisiones de arquitectura, con lo que se descartó y por qué, están en
[`docs/decisiones.md`](docs/decisiones.md). Incluyen las que **no** se harían así en
producción y se hacen así aquí por ser un portafolio, señaladas explícitamente.

## Fuente

Profeco, *Quién es Quién en los Precios*, vía `repodatos.atdt.gob.mx`.
Archivos CSV quincenales, aproximadamente 155 MB cada uno.
