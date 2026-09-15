# Carpeta de items de Fabric

**Esta carpeta la escribe Fabric, no una persona.** `ws-gansito-dev` está conectado por git
integration a la rama `dev` apuntando aquí. Editar estos archivos a mano rompe la sincronización.

La única excepción es `parameter.yml`, que reasigna al desplegar a prod lo que la git integration
guarda como GUIDs de dev: el refresh del modelo, su Direct Lake y los schedules. Qué se reasigna y
por qué, en `docs/plataforma.md`.

Lo que el workspace necesita configurado fuera de git está en [`docs/entorno.md`](../docs/entorno.md).
