import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{to_date, col,datediff,row_number,min,max,lead,lag,coalesce,date_add}

object task2 {
  def main(args:Array[String]):Unit= {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val file = "C:/Users/216784/Documents/dataset/patient data.csv"
    val df = spark.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(file)

    df.createOrReplaceTempView("patient_table")
    //df.show()

    val dfDate = df.withColumn("prescriptiondate", to_date(col("prescriptiondate"),"dd/mm/yyyy"))
    dfDate.createOrReplaceTempView("newpatient_table")

    //1
    spark.sql(
      """SELECT patient_uuid,drug,prescriptiondate,dosage,mg,
        FLOOR(DATEDIFF(CURRENT_DATE(), prescriptiondate)/30) as thirty_day_period,
        SUM(dosage * mg) as total_dosage
        FROM newpatient_table
        GROUP BY patient_uuid,drug,prescriptiondate,dosage,mg,FLOOR(DATEDIFF(CURRENT_DATE(), prescriptiondate)/30)
        ORDER BY patient_uuid,prescriptiondate""").show(50)

    //1
    spark.sql(
      """SELECT patient_uuid, drug, prescriptiondate, mg, quantity,
        SUM(total_usage) OVER (PARTITION BY patient_uuid,drug ORDER BY prescriptiondate) AS cumulative_total_usage
        FROM ( SELECT patient_uuid, drug, prescriptiondate, mg, quantity,
        CASE WHEN DATEDIFF(prescriptiondate, MIN(prescriptiondate) OVER (PARTITION BY patient_uuid)) < 30 THEN mg * quantity ELSE 0 END AS total_usage
        FROM newpatient_table GROUP BY patient_uuid, prescriptiondate, drug, mg, quantity
        ORDER BY patient_uuid, prescriptiondate)""").show(100)

    //2.
    spark.sql("SELECT patient_uuid, drug, prescriptiondate," +
      "CASE WHEN ROW_NUMBER() OVER (PARTITION BY patient_uuid, drug ORDER BY prescriptiondate) = 1 THEN 1 ELSE 0 END AS flag  " +
      "FROM (SELECT patient_uuid, drug, prescriptiondate FROM patient_table " +
      "GROUP BY patient_uuid, drug, prescriptiondate)").show(100)

    //3.
    spark.sql("SELECT patient_uuid, drug, prescriptiondate," +
      "CASE WHEN prescriptiondate = MIN(prescriptiondate) OVER (PARTITION BY patient_uuid, drug) THEN 1 ELSE 0 END AS flag " +
      "FROM (SELECT patient_uuid, drug,prescriptiondate FROM patient_table " +
      "GROUP BY patient_uuid, drug, prescriptiondate " +
      "order by prescriptiondate,patient_uuid)").show(50)

    //4.
    spark.sql("SELECT patient_uuid, drug, prescriptiondate," +
      "datediff(" +
      "coalesce(" +
      "lead(prescriptiondate) OVER (PARTITION BY patient_uuid, drug ORDER BY prescriptiondate)," +
      "lag(prescriptiondate, -1) over(PARTITION BY patient_uuid, drug ORDER BY prescriptiondate)" +
      "),prescriptiondate) AS days " +
      "FROM newpatient_table " +
      "GROUP BY patient_uuid, drug, prescriptiondate " +
      "ORDER BY patient_uuid,prescriptiondate").show(50)



  }
}
