#!bin/sh
echo "initiate"
docker-compose -f ./superset/docker-compose-non-dev.yml pull
docker-compose -f ./superset/docker-compose-non-dev.yml up
