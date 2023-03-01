import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{row_number,when}

object task3 {
  def main(args:Array[String]):Unit= {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val file = "C:/Users/216784/Documents/dataset/claims.csv"
    val df = spark.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(file)

    val file1 = "C:/Users/216784/Documents/dataset/claims_revision.csv"
    val df1 = spark.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(file1)

    df.createOrReplaceTempView("claims_table")
    //df.show()
    df1.createOrReplaceTempView("claims_revision_table")
    //df1.show()

//    val modifiedClaims = spark.sql("SELECT Claim, Line, Date, Line_amt, FND_CD, ROW_NUMBER() OVER (PARTITION BY Claim,Line ORDER BY Date DESC) AS Version, CASE WHEN ROW_NUMBER() OVER (PARTITION BY Claim, Line ORDER BY Date DESC) = 1 THEN 'Y' ELSE 'N' END AS Current_Version_Flag FROM claims_table")
//
//    modifiedClaims.createOrReplaceTempView("output_table")
//    modifiedClaims.show()

    spark.sql("CREATE TABLE output_table (Claim string,  Line int,  Date date,  Line_amt double,  FND_CD string,  Version int, Current_Version_Flag string)USING PARQUET;")

  }
}




