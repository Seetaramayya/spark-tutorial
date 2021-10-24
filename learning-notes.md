# Learning Notes 

### Create Spark Session

Either reading data frame or writing data frame, spark session is required. This can be created in the following way

```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
  .appName("provide nice meaning full name, if you need escape use backticks")
  .config("spark.master", "local[2]") // or master("local[2]") here 2 is number of threads
  .getOrCreate()
```

### Reading DataFrame

- format
- schema or `inferSchama -> true` option
- zero or more options such as 
    - `mode` (there are `failFast`, `dropMalformed` and `permissive` which is default)
    - `compression` -> (`uncompressed`, `bzip2`, `gzip`, `lz4`, `snappy`, `deflate`)
    - `dateFormat` needs to be defined when there are DateType in the source and explicitly if it mentioned 
       otherwise spark returns `null` if they are not in ISO format
      - `MMM dd YYYY` date format works with spark 2.4 and bellow it does not work with spark 3.0 and above,
      this can be make it work by setting `spark.sql.legacy.timeParserPolicy` -> `LEGACY` 
      ```scala
         config("spark.sql.legacy.timeParserPolicy", "LEGACY")
       ```
      or change the format to `MMM d yyyy`
- instead of configuring multiple `.option`s in chained fashion, `.options(Map())` can be used 
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

### Selection and Expression

All possible examples are writen [here](src/main/scala/learn/spark/basics/ColumnsAndExpressions.scala)

- select columns with names 
- Column selection can be done in the following way
  - `col`
  - `column`
  - `expr`
  - `$` string interpolation
  - `'` scala symbols
  

### Aggregations and Grouping

All possible examples are writen [here](src/main/scala/learn/spark/aggregations/AggregationsAndGrouping.scala)

- `count`
- `countDistinct` or `count(distinct(col))`
- `approx_count_distinct`

### Joins

- `inner`
- `left_outer` : everything inner join + all rows in the LEFT table with nulls if the data is missing in the right
- `right_outer`: everything inner join + all rows in the RIGHT table with nulls if the data is missing in the left
- `outer` : everything in the inner join + all the rows in BOTH tables, with nulls if the data is missing
- `left_semi`: everything in the left DF for which there is a row in the right DF satisfying the condition
- `left_anti`: everything in the left DF for which there is NO row in the right DF satisfying the condition

### Filters

Few examples are writen [here](src/main/scala/learn/spark/basics/ComplexTypes.scala)

### Managing Nulls

Few examples are writen [here](src/main/scala/learn/spark/basics/ManageNulls.scala)

- `coalesec`: Returns the first column that is not null, or null if all inputs are null
- `isNull` and `isNotNull` are useful when filtering
- Nulls can be first or last when ordering(`desc_nulls_last` or `asc_nulls_first` etc...)
- To drop nulls `na` can be used. For example `df.select(...).na.drop()`. Don't get confused with `df.select().drop()` 
which removes columns but `na` removes rows
- Replace null with `na.fill...` some values. `df.select().na.fill()`
- Some expr functions which are not intuitive

### Datasets

Few examples are writen in package [dataset](src/main/scala/learn/spark/datasets)

- Datasets are typed data frames or distributed collections of JVM objects
- Use `Datasets` if type safety is important. Use `Dataframes` if performance is important

### Spark SQL

To run spark container use docker compose 

```shell
docker-compose -f ./spark-cluster-docker-compose.yml up
docker exec -it <master-container-name> bash
```

#### SQL Statements

Execute `spark-sql` which gives sql like prompt

```sparksql
show databases;
create database seeta;
use seeta;

-- this creates managed table, that means dropping table means loosing data
create table persons(id integer, name string); 

-- following data will be managed in <>/spark-warehouse/seeta.db/persons/
insert into persons values (1, "Martin Odersky"), (2, "Matei Zaharia"); -- Matei Zaharia who started spark, of course no need to introduce Martin
select * from persons;
describe extended persons;

-- this creates external table that means metadata is not managed by spark 
-- drop table does not loose data
create table flights(origin string, destination string) using csv options(header true,path "/opt/bitnami/seeta/data/flights");
insert into flights values ("Vishakhapatnam", "Amsterdam"), ("Chennai", "Amsterdam");
```

- describe gives following type of output for MANAGED table

```shell
id	int	NULL
name	string	NULL

# Detailed Table Information
Database	seeta
Table	persons
Owner	spark
Created Time	Fri Oct 22 06:22:32 UTC 2021
Last Access	UNKNOWN
Created By	Spark 3.1.2
Type	MANAGED
Provider	hive
Table Properties	[transient_lastDdlTime=1634883758]
Statistics	33 bytes
Location	file:/opt/bitnami/spark-warehouse/seeta.db/persons
Serde Library	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat	org.apache.hadoop.mapred.TextInputFormat
OutputFormat	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Storage Properties	[serialization.format=1]
Partition Provider	Catalog
Time taken: 0.064 seconds, Fetched 20 row(s)
```

- describe gives following type of output for EXTERNAL table

```shell
spark-sql> describe extended flights;
origin	string	NULL
destination	string	NULL

# Detailed Table Information
Database	seeta
Table	flights
Owner	spark
Created Time	Fri Oct 22 06:23:02 UTC 2021
Last Access	UNKNOWN
Created By	Spark 3.1.2
Type	EXTERNAL
Provider	csv
Location	file:/opt/bitnami/seeta/data/flights
Serde Library	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat	org.apache.hadoop.mapred.SequenceFileInputFormat
OutputFormat	org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
Storage Properties	[header=true]
Time taken: 0.065 seconds, Fetched 17 row(s)
```

### RDDs (Resilient Distributed Datasets)

RDD is distributed collections of JVM objects. In general, it is not good to work with RDDs unless we know a lot of spark internals. 
Datasets are sufficient to work with most of the operations. 

#### Comparison between RDDs vs Datasets 

##### in common

- collection API: map, flatMap, filter, take, reduce, etc...
- union, count, distinct
- groupBy, sortBy

##### RDDs over Datasets

- partition control: repartition, coalesec, partitioner, zipPartitions, mapPartitions
- operation control: checkpoint, isCheckpointed, localCheckpoint, cache
- storage control: cache, getStorageLevel, persist

##### Datasets over RDDs

- select and join

Basic RDD creations and conversions are [here](src/main/scala/learn/spark/rdd/BasicRDDs.scala)

- Convert Regular scala collection into RDD
- Read RDD from a file
- Convert Dataset -> RDD
- Convert RDD[T]  -> Dataset[T]
- Convert RDD[T]  -> DataFrame (is an alias to `Dataset[Row]`)

# Resources

- [Submit spark job doc](https://spark.apache.org/docs/latest/submitting-applications.html)
- [RDD](https://databricks.com/glossary/what-is-rdd)