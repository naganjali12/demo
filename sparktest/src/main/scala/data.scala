import org.apache.spark.sql.SparkSession


object data {
  def main(args:Array[String]):Unit= {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    val filePath1="C:/Users/216784/Documents/dataset/Parking_Info.csv"
    val filePath2="C:/Users/216784/Documents/dataset/Station.csv"
    //Chaining multiple options
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath1)
    val df3 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath2)
    //df2.show(false)
    //df2.printSchema()
    //df3.show(false)
    //df3.printSchema()
    df2.createOrReplaceTempView("Parking_Info")
    df3.createOrReplaceTempView("Station")
//    spark.sql("select * from Station").show(15)

    val table = spark.sql("select Parking_Info.id,Parking_Info.STATION_ID,Parking_Info.MONTH,Parking_Info.YEAR,Parking_Info.NUMBER_OF_CARS_PARKED,Parking_Info.PERCENT_FILLED,Station.STATION_NAME,Station.NUMBER_OF_SPACES from Parking_Info inner join Station on Parking_Info.station_id=Station.station_id")
    table.show()

    table.createOrReplaceTempView("jointable")


    data1.que1()
    data1.que2()
    data1.que3()
    data1.que4()
  }
}
