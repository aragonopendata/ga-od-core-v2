=======================
Manual de Mantenimiento
=======================

Este manual es un manual pensado para administradores de sistema (Sysadmin) o microinformáticos en caso para que tengan
una FAQ en caso de problemas.


######
Acceso
######

***********************************************************
Restablecimiento de intentos de autenticación desde consola
***********************************************************

Se deberá acceder a la maquina vía ssh y entrar en el container vía:

.. code-block:: shell

   docker exec -it <nombre de container> /bin/bash

Puedes encontrar el nombre de container mediante:

.. code-block:: shell

   docker ps

Donde aparecerá el nombre del container buscado muy posiblemente ``gaodcore``. También puedes revisar como se ha desplegado
en la documentación de infraestructura.

Una vez hayas accedido al container revisa la documentación oficial:
https://django-axes.readthedocs.io/en/latest/3_usage.html#resetting-attempts-from-command-line
