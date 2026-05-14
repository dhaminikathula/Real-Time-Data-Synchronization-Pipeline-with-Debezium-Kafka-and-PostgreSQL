#!/bin/bash

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
while [ $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/) -ne 200 ] ; do
  echo -e "\tKafka Connect is not ready yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Kafka Connect is ready. Registering Debezium connector..."

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "products-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "user",
    "database.password": "password",
    "database.dbname": "products_db",
    "topic.prefix": "pg-server",
    "database.server.name": "pg-server",
    "table.include.list": "public.products",
    "plugin.name": "pgoutput",
    "tombstones.on.delete": "true"
  }
}'

echo -e "\nDebezium connector registered successfully."
