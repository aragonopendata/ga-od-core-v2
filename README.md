# GAODCORE

_"Gobierno de Aragón Open Data Core"_ is an app that allow to interact with public resources of _Gobierno de Aragón_.

## Terminology

- Resource: any kind of data that user can download. Origin of this data can be produced from only one or mix of APIs,
  database tables, database views, database functions, etc. *View* is deprecated alias of resource. Motives of this
  deprecation are confusion with database views and use a consistent terminology with _CKAN_.
- Connector: any way to connect with external resources.

## APPs

To discover all endpoints please check following
swagger: [https://opendata.aragon.es/GA_OD_Core/ui/](https://opendata.aragon.es/GA_OD_Core/ui/)

- **default**: Public APP that allow that provide public data of _Gobieno de Aragón_.
  Directory: [/GA_OD_Core/](/GA_OD_Core/)
- **transports**: Public APP that provide different endpoints to get data of transports of _Aragón_.
  Directory: [/GA_OD_Core/gaodcore-transports](/GA_OD_Core/gaodcore-transports)
- **admin**: Private APP that allow to manage authentication and authorization. Directory:
  [/GA_OD_Core_admin/admin](/GA_OD_Core_admin/admin)
- **manager**: Private APP that allow to manage _gaodcore_ functionalities. This app is hidden in swagger if you are not
  session authenticated. Directory: [/GA_OD_Core_admin/manager](/GA_OD_Core_admin/manager)

## Usage

### Authentication
Currently, it is allowed Session and Basic authentication.

#### Session Authentication
This is util to show **manager** app in swagger. You can authenticate graphically here: []()

#### Basic Authentication
This is util to deal with manager app. For more information: [https://en.wikipedia.org/wiki/Basic_access_authentication](https://en.wikipedia.org/wiki/Basic_access_authentication)

### Create a new resource

#### Create