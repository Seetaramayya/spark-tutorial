package learn.spark.joins

import org.apache.spark.sql.{ Column, DataFrame, SparkSession }
import org.apache.spark.sql.functions._

object EmployeeJoins {
  private val spark = SparkSession
    .builder()
    .appName("Employee joins")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._
  val user     = "admin"
  val password = "admin"
  val options = Map(
    "driver"   -> "org.postgresql.Driver",
    "url"      -> "jdbc:postgresql://localhost:5432/seeta",
    "user"     -> user,
    "password" -> password
  )
  private def loadTable(name: String): DataFrame =
    spark.read.format("jdbc").options(options).option("dbtable", name).load()

  private val employeeDF          = loadTable("public.employees")
  private val salariesDF          = loadTable("public.salaries")
  private val departmentManagerDF = loadTable("public.dept_manager")
  private val titlesDF            = loadTable("public.titles")

  // Show all employees and their max salary
  private val maxSalaryByEmpNoDF     = salariesDF.groupBy($"emp_no").agg(max($"salary").as("maxSalary"))
  private val employeeNumber: Column = employeeDF.col("emp_no")
  private val employeeWithMaxSalaryDF =
    employeeDF.join(maxSalaryByEmpNoDF, employeeNumber === maxSalaryByEmpNoDF.col("emp_no"))

  // Show all employees who were never managers
  private val employeesWhoAreNotManagersDF =
    employeeDF.join(departmentManagerDF, employeeNumber === departmentManagerDF.col("emp_no"), "left_anti")

  // find the job titles of best paid 10 employees in the company
  private val topPaidEmployeesDF = employeeWithMaxSalaryDF.orderBy($"maxSalary".desc).limit(10)
  private val recentJobTitlesDF  = titlesDF.groupBy($"emp_no", $"title").agg(max($"to_date"))

  def main(args: Array[String]): Unit = {
    // employeeWithMaxSalaryDF.show()
    // employeesWhoAreNotManagersDF.show()
    // topPaidEmployeesDF.show()
    recentJobTitlesDF.show()
    topPaidEmployeesDF.show()

    topPaidEmployeesDF.join(recentJobTitlesDF, "emp_no").orderBy($"maxSalary".desc).show()
  }
}
