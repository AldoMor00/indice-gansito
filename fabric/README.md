# Carpeta de items de Fabric

**Esta carpeta la escribe Fabric, no una persona.**

`ws-gansito-dev` esta conectado por git integration a la rama `dev` apuntando a este
directorio. Los notebooks, pipelines, modelos y reportes se editan en el UI de Fabric
y se commitean desde ahi. Editar estos archivos a mano rompe la sincronizacion.

La unica excepcion es `parameter.yml`, que si se mantiene a mano.

## parameter.yml

Traduce las referencias entre items de dev a prod al desplegar. Todo lo que apunte
de un item a otro —actividades de pipeline, el modelo semantico, el reporte,
las conexiones— necesita su entrada aqui.

**Si se te olvida agregar una entrada, el despliegue pasa en verde apuntando a dev.**
No falla; miente. Es la deuda de mantenimiento real de este esquema.

Los notebooks son la excepcion: no usan lakehouse por defecto y resuelven su ruta en
tiempo de ejecucion con `gansito.contexto`, asi que no necesitan reasignacion.
