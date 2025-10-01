import org.apache.spark.sql.DataFrame

object HelloSpark extends Serializable {

  import org.apache.spark.sql.SparkSession

  def main(args:Array[String])= {
    print("Hello")
    val spark = SparkSession.builder().master("local[1]").getOrCreate()


    def loadSurveyDF(spark: SparkSession, fileName: String): DataFrame = {
      val df = spark.read.format("csv").option("header", true).option("inferSchema", "true").load(fileName)
      df.show()
      df
    }

    def countByCountry(frame: DataFrame):DataFrame={
      frame.select("Age","Gender","Country","state").groupBy("Country").count()
    }


    val surveyDF = loadSurveyDF(spark, args(0))
    val count = countByCountry(surveyDF)
    count.show()
    count.foreach(row=>{
      println("Country :"+" "+row.getString(0)+" "+ "Count :"+ " " +row.getLong(1))
    })
  }

}
