@echo off
echo Starting Node 1 (Also starting embedded ActiveMQ broker)
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8080 --embedded.broker=true"
