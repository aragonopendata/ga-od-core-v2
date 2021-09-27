=======================
Man de Desarrollador
=======================

En este manual es para programadores para que puedan mantener y evolucionar GAODCore.

##############################
Generación de la documentación
##############################

*****************
Instalar Graphviz
*****************

Graphviz es el componente que genera los diagramas y gráficos.

Por ejemplo de este fichero:

.. literalinclude :: aragon_open_data.dot
   :language: graphviz


Genera este gráfico:

.. graphviz:: aragon_open_data.dot

Instalar Graphviz: Windows
==========================
Por realizar.

Instalar Graphviz: Linux
========================

.. code-block:: shell

   sudo apt install graphviz

*****************************
Eliminar los build anteriores
*****************************

Este paso no es estrictamente necesario, solo es necesario cuando se modifican los doctrees y las imágenes generadas por
graphviz.

.. code-block:: shell

    cd docs
    make clean

************************
Generar la documentación
************************

Para generarla en HTML:

.. code-block:: shell

    cd docs
    make html

Para generarla en MarkDown:



#################################
Arrancar el servicio para pruebas
#################################

********************************
Copiar ficheros de configuración
********************************

Puedes copiar el fichero config.template.yaml al directorio de configuración con el nombre ``config-tst.yaml``, que es
nombre por defecto que utiliza. También se puede proporcionar como variable de entorno ``CONFIG_PATH`` con el nombre de
fichero.

Revisar: :ref:`Configurar fichero de configuración`


Copiar ficheros de configuración: Linux
=======================================

El directorio de configuración es: ``/etc/gaodcore/``.

Copiar ficheros de configuración: Windows
=========================================
El directorio de configuración es: ``C:\\ga-od-core-v2``

******************
Probar el servicio
******************

Con docker
==========
Seguir la documentación de infrastructura.


Sin docker
==========

Crear entorno virtual
---------------------

.. code-block:: shell

    python3 -m venv venv

Activar virtualenv
------------------

Activar virtualenv: Linux
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: shell

    source venv/bin/activate

Activar virtualenv: Windows
^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO:

.. code-block:: shell

    source venv/bin/activate


Instalar librerías
------------------

Instalar librerías: desarrollador
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


En caso de querer todas las librerías para hacer uso de la aplicación lanzar los test, crear la documentación.

.. code-block:: shell

    pip install -r requirements-dev.txt

Instalar librerías: no desarrollador
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: shell

    pip install -r requirements.txt


##################
Lanzar los pruebas
##################

Revise: ref:`Instalar librerías: desarrollador`

Ejecute:

.. code-block:: shell

    tox

Se ejecutaran todas las validaciones con diferentes configuraciones configuradas para tox.

############
Arquitectura
############

***********
Componentes
***********

.. graphviz:: infrastructura.dot


Proceso de respuesta de una respuesta de un usuario externo
===========================================================

Apache de Aragonesa de servicios telemáticos (AST)
--------------------------------------------------

Cuando llega una petición a AST se evalua si el dominio es ``opendata.aragon.es``. Se es asi se el servidor web se
comporta como proxy realizando una petición a Aragón Open Data por el puerto 7030 con tráfico http.


Apache de Aragón Open Data
--------------------------

Cuando llega al servidor web de aragón opendata por el puerto 7030 valida que el directorio raíz es ``GA_OD_Core``, por
ejemplo: ``http://opendata.aragon.es/GA_OD_Core/ui``.

Si se cumplen las anteriores condiciones este servidor se comporta como proxy realizando una petición a la interficie
de loopback por el puerto 6001 con trafico http.

Nota importante: GA_OD_Core_admin no se puede consultar desde el servidor web de Aragón Open


Gunicorn del docker de GA_OD_Core
---------------------------------

El puerto 6001 de la maquina amfitriona esta mapeado con el puerto 8000 del container de GA_OD_Core. Este mapeo lo
realiza el servicio de docker.

Gunicorn al recibir la petición con el puerto con el puerto 8000 ejecuta el GA_OD_Core. A través de la interficie
(WSGI) implementada.

*************
Autenticación
*************

.. graphviz:: auth.dot

Solamente se requiere de autenticación para aquellas api que requieren de usuario registrado, la mayoría de las API no
requieren estar autenticados. Solo aquellas que estan en el directorio raíz ``GA_OD_Core_admin`` requieren de estar
autenticadas para hacer uso de las APIs.

axes.backends.AxesBackend
=========================

Este componente se encarga de verificar que no se realiza un ataque de fuerza bruta para obtener los passwords. Si se
llega al maximo de intentos por IP o por nombre de usuario se bloquea la cuenta o la IP.
Mirar la documentación de Axes para mas información.

Para saber cuantas veces se ha conectado por una IP o por nombre de usuario, se guardan en una base de datos relacional,
debido a la falta de tiempo no se ha podido usar un mecanismo mas adecuado como por ejemplo usar redis.

Solamente cuando se valida una petición correcta le pasa al testigo a ``django.contrib.auth``.

django.contrib.auth
===================

Este componente es el que se encarga de realizar la autenticación.

******************
Obtención de datos
******************

.. graphviz:: data_flow.dot

Se realiza la petición
======================

Se gestiona la petición

Obtención de la configuración
=============================

``Django Rest Framework`` es utilizado para obtener crear dos endpoints REST.

.. autoclass:: gaodcore_manager.models.ConnectorConfig
    :members:

GAODCore
========

1 - La mayoría de procesos que deben interactuar con la configuración,



