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
