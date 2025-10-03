object Streaming extends Serializable{

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.streaming.Trigger

  def main(args:Array[String])={

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spar.sql.shuffle.partitions","3")
      .getOrCreate()

    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","invoices")
      .option("startingOffsets","earliest")
      .load()

    val formatdf = df.select(from_json(col("value").cast("string"),schema).alias("value"))

    val explodedf = formatdf.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

     val flattenedDF = explodedf.withColumn("Itemcode",expr("LineItem.Itemcode"))
       .withColumn("ItemDescription",expr("LineItem.ItemDescription"))

     val writinvoiceWriterQuery = flattenedDF.writeStream
       .outputMode("append")
       .format("json")
       .option("path","output-kafka")
       .option("checkpointLocation","chk-point-dir-kafka-1")
       .trigger(Trigger.ProcessingTime("1 minute"))
       .start()

    writinvoiceWriterQuery.awaitTermination()





  }
}
