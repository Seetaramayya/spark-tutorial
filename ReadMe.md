# Learning Spark

- net cat 
```shell
 cat people-1m.txt| nc -lk 9090
```

### Reading DataFrame

- format 
- schema or `inferSchama -> true` option
- zero or more options such as `mode` (there are `failFast`, `dropMalformed` and `permissive` which is default)
  - instead of sending multiple chained `option`, `options(Map())` can be used
- load(path)

```scala
val dataframe = spark.read
               .format("json")
               .option("inferSchema", "true") // or .schema(StructType(Array(StructField(), ...))
               .option("mode", "failFast") // default is permissive
               .load("src/main/resources/data/some.json")
```

### Writing DataFrame

- format
- save mode = overwrite, append, ignore, errorIfExist
- path
- zero or more options

NOTE: creates directory instead of file 
```scala
carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars/cars-duplicate.json")
```


# Resources

- [Structured streaming kafka integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Spark Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)
- [Spark streaming Example](https://github.com/sparkbyexamples/spark-examples/blob/master/spark-streaming/src/main/scala/com/sparkbyexamples/spark/streaming/kafka/json/SparkStreamingConsumerKafkaJson.scala)
- [Spark streaming with Kafka](https://sparkbyexamples.com/spark/spark-streaming-with-kafka/)