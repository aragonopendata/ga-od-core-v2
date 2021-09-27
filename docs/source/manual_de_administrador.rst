========================
Man de Administración
========================

En este manual es para usuarios internos no técnicos de la organización con derechos de administración en GAODCore.

#########
Seguridad
#########
Hay varias capas de seguridad para evitar ataques maliciosos que puedan proceder tanto de forma interna como externa a
la organización.

***************
Acceso limitado
***************
Solo los usuarios conectados via VPN y que tengan conectividad por http a la maquina en la que esta desplegada GAODCore
podrán gestionar la aplicación.

*****************************************
Protección contra ataques de fuerza bruta
*****************************************

Hay una cantidad máxima de intentos para poder autenticarse en GAODCore. Hay limites tanto por nombre de usuario y por
IP. Para conocer exactamente cuales son los limites consultar la <configuración desplegada>.

Para restablecer tiene las siguientes opciones:

- :ref:`Restablecimiento de intentos de autenticación desde el Panel de Administración`
- :ref:`Restablecimiento de intentos de autenticación desde consola`

*******************
Contraseñas seguras
*******************
Los administradores son los encargados de que sus contraseñas sean seguras y se cambian con regularidad.

****************************
Permisos de las credenciales
****************************
Las credenciales proporcionadas por el proveedor de datos tienen que tener los permisos limitados para que solo se
puedan obtener los datos que se quieran publicar.

##########################
Administración de GAODCore
##########################

Toda la administración del portal se accede a través unas urls que deberían no ser accesibles desde el exterior, las cuáles tienen el
siguiente formato:

- ``http://host:port/GA_OD_Core_admin/ui``: para acceder de otra manera al swagger. Tiene las mismas características que en `Swagger público <https://opendata.aragon.es/GA_OD_Core/ui/>`_ . Solo se añadido por comodidad ya que *Aragonesa de servicios telemáticos* nos restringe algunas funcionalidades en producción.
- ``http://host:port/GA_OD_Core_admin/admin``: panel de administración.
- ``http://host:port/GA_OD_Core_admin/manager/*``: todas las API's de administraciones.


***********************
Panel de Administración
***********************

El Panel de Administración te permite gestionar de forma grafica tareas de administración

Para acceder puede tener la referencia de las siguiente URL: ``http://host:port/GA_OD_Core_admin/admin``


.. image:: _static/admin/home.png
   :alt: Administrator panel


Autenticación y Autorización
============================

En este apartado te permite gestionar los usuarios y los grupos.

En un futuro esperamos poderlo integrar con LDAP y/o Kerberos y evitar multiples proveedores de identidad y/o de
autorización.

Creación, modificación y eliminación de usuarios
------------------------------------------------

.. image:: _static/admin/authn_authz/user_step_1.png
   :alt: Add user step 1

.. image:: _static/admin/authn_authz/user_step_2.png
   :alt: Add user step 2

Notas:

- La segunda parte y la modificación de un usuario son comunes.
- Para eliminar un usuario modificar el usuario y al final del formulario donde podrá visualizar el boton para eliminar el usuario.
- Actualmente para hacer uso de las APIs de manager no se requiere permisos en especificos solo estar autenticado.

Creación, modificación y eliminación de grupos
----------------------------------------------

Los grupos sirven para agrupar diferentes usuarios, y asignando un permiso a un grupo todos los usuarios de ese grupo obtengan ese permiso.

.. image:: _static/admin/authn_authz/group.png
   :alt: Group


Axes
====

Este es un apartado de seguridad, sobre la autenticación de los usuarios.

Access attempts
---------------
Este apartado muestra los intentos fallidos de autenticación. El uso mas frecuente es para detectar posibles ataques,
puedes encontrar información interesante como la IP y el usuario que ha fallado al realizar la autenticación.

Resumen:

.. image:: _static/admin/axes/access_attempts/home.png
   :alt: Access attempts

Reporte:

.. image:: _static/admin/axes/access_attempts/report.png
   :alt: Access report


Restablecimiento de intentos de autenticación desde el Panel de Administración
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Seleccione los intentos que desea permitir nuevamente y simplemente elimínelos. El usuario bloqueado podrá volver a iniciar sesión de acuerdo con las reglas.
Lo puede realizar a traves del reporte pulsando eliminar o desde el resumen:

.. image:: _static/admin/axes/access_attempts/reset.png
   :alt: Access report

Seleccionando la acción y pulsando ir.

Access logs
-----------

En este apartado se puede monitorizar las diferentes autenticaciones.

.. image:: _static/admin/axes/access_logs/logs.png
   :alt: Access report

****************
API's de Manager
****************

Las API's de Manager estan pensadas para la publicación de recursos de forma manual y de forma automatizada. A modo
de resumen tenemos el siguiente workflow:

- :ref:`Obtención de la URI`
- :ref:`Obtención de la localización del recurso`
- :ref:`Validación`
- :ref:`Creación de la configuración de la conexión`
- :ref:`Creación de la configuración del recurso`

Puede consultar los :ref:`Conceptos` pulsando en el link.

Es interesante tener conocimiento de como funciona una API REST y de los métodos http, pero a modo de
resumen:

Métodos HTTP:

- GET: Obtiene una representación del estado del recurso de destino.
- POST: Deje que el recurso de destino procese la representación incluida en la solicitud.
- PUT: Crea o reemplaza el estado del recurso de destino con el estado definido por la representación incluida en la solicitud.
- DELETE: Eliminar el estado del recurso de destino.

Obtención de la URI
===================

Actualmente solo se permiten URLs el formato de una URI es la siguiente:

``schema://username:password@host/recurso``

Ejemplos:

``postgresql://usuario:contraseña@aragon.es/topsecret``

``oracle://usuario:contraseña@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=aragon.es)(PORT=00001)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=TOPSECRET)))``

Requisitos de la URI
--------------------

Proveedor de datos
^^^^^^^^^^^^^^^^^^

Proveedor de datos: RDBMS
"""""""""""""""""""""""""

Actualmente solo se da soporte a RDBMS (Sistema de gestión de bases de datos relacionales) siguientes:

- postgresql (con pruebas automatizadas)
- mysql/mariadb (con pruebas automatizadas)
- oracle (pruebas manuales, debido a problemas de licencia)
- mssql (sin probar, debido a problemas de licencia)

Aunque no se hayan realizado pruebas es muy posible que funcione con los siguiente dialectos:

https://docs.sqlalchemy.org/en/14/dialects/

Requisitos de la URI: Proveedor de datos: APIs
""""""""""""""""""""""""""""""""""""""""""""""

Actualmente solo se ofrece soporte bases de datos y API's que devuelvan CSV y/o JSON (que estén aplanados). Solo se
realiza peticiones GET.

Requisitos de la URI: Privilegios
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Requisitos de la URI: Privilegios: RDBMS
""""""""""""""""""""""""""""""""""""""""

Es realmente importante seguir las directivas marcadas de este punto: :ref:`Permisos de las credenciales`.

Los privilegios que se requieren para RDBMS son los siguientes:

- Permiso de conexión

Requisitos de la URI: Privilegios: APIS
"""""""""""""""""""""""""""""""""""""""

Los privilegios que se requieren para RDBMS son los siguientes:

- Permisos de red.
- Permisos para realizar peticiones GET.

Obtención del nombre del recurso


Obtención de la localización del recurso
========================================

Este apartado solo es aplicable para fuentes de datos del tipo RDBMS. Cuando se pregunte por los datos dejarlo en
blanco.

Hay dos partes necesarias para este punto:

- Object location: es el nombre de la tabla o vista.
- Object location schema: es el schema de la tabla o vista. Puede estar vacio en caso que el RDBMS no soporte esta funcionalidad.

Obtención de la localización del recurso: Privilegios
-----------------------------------------------------
Los permisos necesarios son los siguientes:

- Permiso de ``SELECT`` del objeto (tabla o view) a publicar.
- Permiso de como se ha creado el objeto:

 - MySQL:

  - ``SHOW CREATE TABLE``


Obtención de la localización del recurso: Requisitos
----------------------------------------------------

Limitaciones
^^^^^^^^^^^^

Debido a que Excel tiene unas limitaciones: https://support.microsoft.com/en-us/office/excel-specifications-and-limits-1672b34d-7043-467e-8e27-269d656771c3
pasará X (Por determinar con Julián)


Validación
==========

La API de validación es una API de validación para antes que se añada cualquier configuración de conexión o configuración
de recurso, se pueda probar si hacer cambios persistentes.

.. include:: _templates/important_manager_notes.rst

TODO: Añadir imagen

Creación de la configuración de la conexión
===========================================

La creación de la configuración conexión permitirá que GAODCore pueda conectarse a una fuente de datos: una BD o un
servidor http.

.. include:: _templates/important_manager_notes.rst


.. image:: _static/api_manager/create_connector.png
   :alt: Create connector

Creación de la configuración del recurso
========================================

La creación de la configuración permitirá que GAODCore pueda ofrecer el recurso al público. GAODCore se conectará con
la configuración de la conexión y será capaz de obtener los datos con la configuración del recurso.

.. include:: _templates/important_manager_notes.rst

.. image:: _static/api_manager/create_resource.png
   :alt: Create connector
