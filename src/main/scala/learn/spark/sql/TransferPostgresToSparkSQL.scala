package learn.spark.sql

import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }

import scala.util.Try

object TransferPostgresToSparkSQL {
  private val tableNames = List("departments", "dept_emp", "dept_manager", "employees", "salaries", "titles")
  private val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", "target/warehouse")
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") // only for spark 2.4
    .appName("Transfer Postgres -> SparkSQL")
    .master("local[2]")
    .getOrCreate()

  private def readPostgresTable(tableName: String): DataFrame = spark.read
    .format("jdbc")
    .options(
      Map(
        "driver"   -> "org.postgresql.Driver",
        "url"      -> "jdbc:postgresql://localhost:5432/seeta",
        "user"     -> "admin",
        "password" -> "admin",
        "dbtable"  -> tableName
      )
    )
    .load()

  private def loadJson(fileName: String, basePath: String = "src/main/resources/data"): DataFrame =
    spark.read.option("inferSchema", "true").json(s"$basePath/$fileName")

  private def transferTable(tableName: String): Unit = {
    val tableDF = readPostgresTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    Try(tableDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName))
  }

  private def transferTables(tableNames: List[String]): Unit = tableNames.foreach(transferTable)

  def main(args: Array[String]): Unit = {
    spark.sql("create database seeta")
    spark.sql("use seeta")
    transferTables(tableNames)

    // TODO: Why? Overwrite is there why it is complaining?
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Can not create the managed table('`movies`').
    // The associated location('target/warehouse/movies') already exists.

    // Exercise 1. Read the movies DF and store it as a Spark table in the seeta database.
    // val moviesDF = loadJson("movies.json")
    // moviesDF.createOrReplaceTempView("movies")
    // moviesDF.write.mode(SaveMode.Overwrite).saveAsTable("movies")

    // Exercise 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    spark
      .sql("""
             | select count(*) as hired_in_1999 from employees where hire_date >= '1999-01-01' and hire_date < '2000-01-01'
             |""".stripMargin)
      .show()

    spark
      .sql("""
        | select * from employees
        |""".stripMargin)
      .show()

    spark
      .sql("""
             | select * from salaries
             |""".stripMargin)
      .show()

    spark
      .sql("""
             | select * from dept_emp
             |""".stripMargin)
      .show()

    // Exercise 3: Show the average salaries for the employees hired in between those dates, grouped by department.
    spark
      .sql("""
             | select dept_no, avg(s.salary)
             |   from employees e, salaries s, dept_emp de
             |   where (e.hire_date >= '1999-01-01' and e.hire_date < '2000-01-01') and 
             |         e.emp_no = s.emp_no and 
             |         e.emp_no = de.emp_no 
             |   group by de.dept_no
             |         
             |""".stripMargin)
      .show()

    // Exercise 4: Show the name of the best-paying department for employees hired in between those dates.
    spark
      .sql("""
             | select d.dept_name, avg(s.salary) average_salary
             |   from employees e, salaries s, dept_emp de, departments d
             |   where (e.hire_date >= '1999-01-01' and e.hire_date < '2000-01-01') and 
             |         e.emp_no = s.emp_no and 
             |         e.emp_no = de.emp_no and 
             |         d.dept_no = de.dept_no
             |   group by d.dept_name
             |   order by average_salary desc
             |   
             |""".stripMargin)
      .show()

  }
}
