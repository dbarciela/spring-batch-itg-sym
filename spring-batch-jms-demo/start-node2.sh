#!/bin/bash
echo "Starting Node 2"
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8081"
