# Decisiones

Qué se decidió, qué se descartó y por qué. Cuando algo no se haría así en un sistema
productivo, se dice aquí mismo en vez de dejarlo implícito.

## 1. La zona raw vive fuera de Fabric

El histórico se guarda en `indice-gansito-datos`, escrito por GitHub Actions y leído
por Fabric. OneLake vive dentro de la capacidad, y la capacidad es una trial que va a
desaparecer. Con el raw afuera, Fabric queda desechable: se borra el workspace y se
reconstruye sin perder un día de historia.

*En producción:* el raw viviría en ADLS con política de ciclo de vida. Un repositorio
de git no es un almacén de datos.

## 2. Se filtra en la puerta

De cada CSV de ~155 MB se persisten dos cortes: las filas del catálogo objetivo y las
tuplas distintas de tienda. Las tiendas salen del **archivo completo**, no de las filas
ya filtradas, para que `dim_tienda` no quede sesgada a las tiendas que venden pastelillos.

Esto **rompe la inmutabilidad del raw** y es la concesión más grande del proyecto: un
raw filtrado ya no permite rehacer un análisis distinto sin volver a la fuente. Se
mitiga guardando en el manifiesto el `sha256` y la URL de origen de cada archivo.

*En producción:* se aterriza el archivo íntegro y el filtro es la primera transformación.

## 3. Los notebooks no usan lakehouse por defecto

El enlace del UI guarda el **GUID** del lakehouse. Al desplegar a otro workspace ese
GUID sigue apuntando al origen, y el notebook corre en verde sobre los datos
equivocados. En vez de eso, los notebooks leen su workspace en tiempo de ejecución y
arman la ruta ABFSS con nombres.

Lo que sí hay que reasignar es lo que apunta de un item a otro —pipelines, modelo
semántico, reporte, conexiones— y eso vive en `fabric/parameter.yml`.

*En producción:* igual. Esto no es una concesión, es como debería hacerse siempre.

## 4. Prod no está conectado a git

`ws-gansito-dev` tiene git integration con `dev`: es la única sincronización de dos
vías del proyecto. A `ws-gansito-prod` se le **despliega** desde `main` con
`fabric-cicd`, en GitHub Actions, con credencial federada OIDC y sin ningún secreto
guardado. Rollback = revertir el commit y dejar que el job corra otra vez.

`unpublish_all_orphan_items` no se llama: haría de prod un espejo exacto del repo,
hasta el día que borre un lakehouse y se lleve los datos.

## 5. Dos ambientes, sin test

Dev no ingesta: tiene un shortcut de OneLake al `lh_bronze` de prod, de sólo lectura, y
sus propios silver y gold para escribir. Una sola ingestión alimenta a los dos, y dev
ya desarrolla contra datos reales — que es lo que un ambiente de test iría a comprobar.

Riesgo asumido: un cambio en la forma de bronze rompe dev de inmediato, sin ambiente
intermedio. Con un solo desarrollador es aceptable.

## 6. Dos modelos semánticos

Un modelo Direct Lake no se puede mover a una cuenta gratuita: exige capacidad y muere
cuando se apaga. Así que hay dos. El de Fabric es Direct Lake sobre `lh_gold`. El
público es un PBIX en modo import que lee los agregados exportados a CSV en el repo de
datos, por URL anónima. El costo es que el DAX vive duplicado.

## 7. Nada de wheels: `%run` y pruebas en notebook

Empaquetar la lógica en una wheel y colgarla de un Environment de Fabric es "lo
correcto" y en la práctica lo sostiene un equipo de plataforma: el ciclo de publicar un
Environment son minutos y mata la iteración. Aquí los notebooks comparten helpers con
`%run nb_00_config`.

Eso deja dos tipos de prueba, cada una donde corresponde:

- **De código** (¿la normalización devuelve lo esperado?): `nb_90_pruebas`, asserts
  contra pares de entrada y salida conocidos.
- **De datos** (¿cuadra el conteo, hay nulos donde no debe?): `nb_40_dq`. No truenan
  con un assert suelto — escriben su veredicto en `dq_resultados`.

El Python que corre en GitHub Actions (la ingesta) es un caso aparte: nunca entra a
Fabric, así que ahí sí hay `pytest` y `ruff` normales. Llega en la fase F1.
