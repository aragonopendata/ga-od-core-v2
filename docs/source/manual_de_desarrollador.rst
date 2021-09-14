=======================
Manual de Desarrollador
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

    make html

Para generarla en MarkDown:

.. code-block:: shell

    make md

#########
Desarollo
#########

********************************
Copiar ficheros de configuración
********************************

Puedes copiar el fichero config.template.yaml al directorio de configuración con el nombre ``config-tst.yaml``, que es
nombre por defecto que utiliza. También se puede proporcionar como variable de entorno ``CONFIG_PATH`` con el nombre de
fichero.

.. automodule: config


Copiar ficheros de configuración: Linux
=======================================

El directorio de configuración es: ``/etc/gaodcore/``.

Copiar ficheros de configuración: Windows
=========================================
El directorio de configuración es: ``C:\\ga-od-core-v2``







*************************
Arrancar servicio de test
*************************


############
Arquitectura
############

.. graphviz:: infrastructura.dot

***************
Infraestructura
***************

.. graphviz:: auth.dot

