====================
Manual de Despliegue
====================

En este manual se explicara como desplegar y actualizar GAODCore.

##################
Crear BD y usuario
##################
Crear una bd y un usuario que tenga permisos para crear tablas.


########################################################
Descargar o actualizar el repositorio de Infraestructura
########################################################
Revisar si ya existe el repositorio de Infraestructura.

.. code-block:: bash

   ls /opt/infraestructura/

Si no existe ir a :ref:`Descarga el repositorio de Infraestructura`. Si existe ir a
:ref:`Actualizar el repositorio de Infraestructura`

******************************************
Descarga el repositorio de Infraestructura
******************************************

.. code-block:: bash

   cd /opt
   git clone https://github.com/aragonopendata/infraestructura.git

********************************************
Actualizar el repositorio de Infraestructura
********************************************

.. code-block:: bash

   cd /opt/infraestructura/
   git pull

#################################################
Descargar o actualizar el repositorio de GAODCore
#################################################
Revisar si ya existe el repositorio de Infraestructura.

TODO: revisar el directorio una vez tenerlo en PRO por el tema de v2.

.. code-block:: bash

   ls /opt/ga-od-core-v2

Si no existe ir a :ref:`Descarga el repositorio de GAODCore`. Si existe ir a
:ref:`Actualizar el repositorio de GAODCore`

***********************************
Descarga el repositorio de GAODCore
***********************************

.. code-block:: bash

   cd /opt
   git clone https://github.com/aragonopendata/ga-od-core-v2.git

*************************************
Actualizar el repositorio de GAODCore
*************************************

.. code-block:: bash

   cd /opt/ga-od-core-v2/
   git pull


##############################################
Crear o actualizar el fichero de configuración
##############################################

Revisar si ya existe el repositorio de Infraestructura.

.. code-block:: bash

   ls /etc/gaodcore/config.yaml

**********************************
Copiar el fichero de configuración
**********************************

El fichero de configuración tienen el siguiente formato:

.. literalinclude :: ../../config.template.yaml
   :language: yaml

Nota: si se ha modificado el ``config.yaml`` respecto a la versió ya instalada se puede optar por modificar el yaml
ya copiado.

***********************************
Configurar fichero de configuración
***********************************

En este apartado se explica como se debe configurar los diferentes valores del fichero de configuración. Ejemplo:

.. code-block:: bash

   sudo mkdir /etc/gaodcore
   sudo cp /opt/gaodcore/config.template.yaml /etc/gaodcore/config.yaml


Common Config
=============

Configuración común para toda la aplicación

Allowed hosts
-------------

Una lista de cadenas que representan los nombres de dominio/host que este sitio de Django puede servir. Esta es una
medida de seguridad para evitar ataques de encabezado de host HTTP, que son posibles incluso bajo muchas configuraciones
de servidor web aparentemente seguras. Ejemplo:

.. code-block:: yaml

   allowed_hosts:
    - 127.0.0.1

Para más información: https://docs.djangoproject.com/en/3.2/ref/settings/#allowed-hosts

Secret key
----------

Se utiliza para proporcionar firma criptográfica y debe establecerse en un valor único e impredecible.

Puede generar la secret key:

.. code-block:: bash

   tr -dc 'A-Za-z0-9!"#$%&'\''()*+,-./:;<=>?@[\]^_`{|}~' </dev/urandom | head -c 50


Para más información https://docs.djangoproject.com/en/3.2/ref/settings/#allowed-hosts

Debug
-----

Un booleano que activa / desactiva el modo de depuración.

Para más información: https://docs.djangoproject.com/en/3.2/ref/settings/#debug

Databases
---------

Un diccionario que contiene la configuración de la base de datos (default) que se utilizará con Django. Es un
diccionario anidado cuyo contenido mapear un alias de base de datos a un diccionario que contiene las opciones para
una base de datos individual.

.. code-block:: yaml

    databases:
      default:
        ENGINE: 'django.db.backends.postgresql_psycopg2'
        NAME: 'nombre base de datos'
        USER: 'usuario'
        PASSWORD: 'contraseña'
        HOST: 'host'
        PORT: 5433

Para más información: https://docs.djangoproject.com/en/3.2/ref/settings/#databases

Cache TTL
---------

Numero de segundos hasta que una respuesta cacheada se considere invalida. **Afecta a todas las respuestas que tengan cache.**

Para más información: https://docs.djangoproject.com/en/3.2/topics/cache/#django.views.decorators.cache.cache_page

Transports
==========

Confidencial, revisar los documentos de infraestructura.

####################
Arrancar el servicio
####################

TODO: revisar si al final se va hacer uso de dockerhub por ejemplo.

.. code-block:: bash

   cd /opt/infraestructura/
   docker-compose up -d --build
