# Carpeta de items de Fabric

**Esta carpeta la escribe Fabric, no una persona.** `ws-gansito-dev` esta conectado por
git integration a la rama `dev` apuntando aqui. Editar estos archivos a mano rompe la
sincronizacion.

La unica excepcion es `parameter.yml`, que traduce las referencias entre items al
desplegar a prod. Todo lo que apunte de un item a otro necesita su entrada ahi.
**Si se te olvida, el despliegue pasa en verde apuntando a dev:** no falla, miente.

Los notebooks son la excepcion: resuelven su ruta en tiempo de ejecucion, asi que no
necesitan reasignacion.
