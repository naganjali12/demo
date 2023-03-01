import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object data1
{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  // Define the UDF
  def getMonthName(n: Int): String = n match {
    case 1 => "January"
    case 2 => "February"
    case 3 => "March"
    case 4 => "April"
    case 5 => "May"
    case 6 => "June"
    case 7 => "July"
    case 8 => "August"
    case 9 => "September"
    case 10 => "October"
    case 11 => "November"
    case 12 => "December"
    case _ => "Invalid month number"
  }
  // Register the UDF
  val getMonthNameUDF = udf(getMonthName _)
  spark.udf.register("get_month_name_udf", getMonthNameUDF)

  def que1()={
    spark.sql("SELECT STATION_NAME,SUM(NUMBER_OF_CARS_PARKED) AS total_cars,get_month_name_udf(Month) AS month_name " +
      "FROM jointable GROUP BY STATION_NAME, MONTH ORDER BY MONTH").show()
  }

  def que2()={
    spark.sql("SELECT STATION_NAME, SUM(NUMBER_OF_CARS_PARKED) AS total_cars,YEAR,MONTH FROM jointable GROUP BY STATION_NAME,YEAR,MONTH ORDER BY total_cars DESC").show()
  }

  def que3()={
    spark.sql("WITH cumulative_total AS (SELECT STATION_NAME,SUM(NUMBER_OF_CARS_PARKED) AS total_cars,YEAR,MONTH,SUM(SUM(NUMBER_OF_CARS_PARKED)) OVER (PARTITION BY STATION_NAME ORDER BY YEAR,MONTH) AS cumulative_sum FROM jointable GROUP BY STATION_NAME,YEAR,MONTH) SELECT STATION_NAME,total_cars,YEAR,MONTH,cumulative_sum FROM cumulative_total").show(200)
  }
  def que4()={
    spark.sql("WITH cumulative_total AS (SELECT STATION_NAME,MONTH,YEAR,NUMBER_OF_CARS_PARKED,SUM(NUMBER_OF_CARS_PARKED) OVER (ORDER BY MONTH) AS cumulative_sum FROM jointable) SELECT STATION_NAME,YEAR,MONTH,NUMBER_OF_CARS_PARKED,cumulative_sum FROM cumulative_total").show(200)
}

}
