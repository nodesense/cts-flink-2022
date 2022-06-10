# Schema Registry

1. REST API
5. Open the browser in remote desktop, check http://localhost:8081
6. To get all the subjects, <<TOPICNAME-key>> or <<TOPICNAME-value>> http://localhost:8081/subjects
7. To get all the versions of specific subjects http://localhost:8081/subjects/products-value/versions
8. To get schema specific to a version, http://localhost:8081/subjects/products-value/versions/1    where as 1 may change, refer step 7 for exact schema version
8. To get the schema from schema registry using schema id  http://localhost:8081/schemas/ids/1

  
### Avro consoler producer
  
  ```
  kafka-avro-console-producer --broker-list localhost:9092 --topic products   --property value.schema='{"type":"record","name":"product","fields":[{"name":"name","type":"string"},{"name":"price","type":"int"}]}'

  ```
  
### Avro console consumer

 ```
 kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic products --from-beginning --property schema.registry.url="http://localhost:8081"
  ```
  

  ###   console consumer

 ```
 kafka-console-consumer --bootstrap-server localhost:9092 --topic products --from-beginning  
  ```
  

  copy and paste them into producer
  
```
{"name":"iphone", "price" : 1000}
{"name":"galaxy", "price" : 600}
  
```
