# Learning Spark

  All the learning points are noted down [here](./learning-notes.md) for future references.

### PostgreSQL commands

```shell
docker compose up postgres # to start the container
docker exec -it postgres psql -U admin -d seeta # to connect to PSQL


# to find all the table names (except system tables)
select * from information_schema.tables where table_schema not in ('information_schema', 'pg_catalog');
select * from information_schema.tables where table_schema = 'public';
select * from information_schema.columns where table_schema = 'information_schema';

# to describe table or use information_schema 
\d employees
select column_name, data_type, is_nullable from information_schema.columns where table_name = 'employees';

# to find all tables 
\dt
```


### Docker Commands 

```shell
docker-compose -f ./spark-cluster-docker-compose.yaml up
docker exec -it spark-tutorial_spark_1 bash
```
### TODOs 

- In [PostgreSQL#L33](src/main/scala/learn/spark/basics/PostgreSQL.scala), I was expecting more records in database but fewer records exists, needs to be verified 

# Resources

- [Structured streaming kafka integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Spark Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)
- [Spark streaming Example](https://github.com/sparkbyexamples/spark-examples/blob/master/spark-streaming/src/main/scala/com/sparkbyexamples/spark/streaming/kafka/json/SparkStreamingConsumerKafkaJson.scala)
- [Spark streaming with Kafka](https://sparkbyexamples.com/spark/spark-streaming-with-kafka/)
- [Connect to MongoDB](https://mongodb.github.io/mongo-java-driver/4.2/driver-scala/tutorials/connect-to-mongodb/)
