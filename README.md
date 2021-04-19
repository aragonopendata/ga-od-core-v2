docker build -t ga_od_core .
docker -d -p 80:80 ga_od_core