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

    val modifiedClaims = spark.sql("SELECT Claim, Line, Date, Line_amt, FND_CD FROM claims_table")

    modifiedClaims.createOrReplaceTempView("output_table")
    modifiedClaims.show()

   spark.sql("""SELECT c.Claim, c.Line, c.Date, c.Line_amt, c.FND_CD,
  ROW_NUMBER() OVER (PARTITION BY C.Claim, C.Line ORDER BY COALESCE(CR.Revion_Order, 0) DESC)  AS Version,
  CASE WHEN cr.Revion_Order = MAX(cr.Revion_Order) OVER (PARTITION BY c.Claim, c.Line) THEN 'Y' ELSE 'N' END AS Current_Version_flag
FROM claims_table c
LEFT JOIN claims_revision_table cr
  ON c.Claim = cr.Claim AND c.Line = cr.Line
UNION ALL
SELECT cr.Claim, cr.Line, cr.Date, cr.Line_amt, cr.FND_CD,
  ROW_NUMBER() OVER (PARTITION BY CR.Claim, CR.Line ORDER BY CR.Revion_Order DESC) + 1 AS Version,
  CASE WHEN CR.Revion_Order = MAX(CR.Revion_Order) OVER (PARTITION BY cr.Claim, cr.Line) THEN 'Y' ELSE 'N' END AS Current_Version_flag
FROM claims_revision_table cr
ORDER BY Claim, Line, Version;

""").show()

  }
}




