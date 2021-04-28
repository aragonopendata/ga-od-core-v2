docker build -t ga_od_core .
docker -d -p 80:80 ga_od_core

'src/api_reader/config_template.yaml' rellenarlo con las configuración necesaria y renombralo a 'config.yaml'