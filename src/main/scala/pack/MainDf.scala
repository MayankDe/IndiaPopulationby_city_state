package pack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MainDf {

  def mainDf(path: String): Unit = {
    val conf = new SparkConf()
      .set("spark.driver.allowMultipleContexts", "true")
      .setAppName("Practice")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits


    val df = spark.read.format("csv").option("header", true)
      .load(path).drop("lat", "lng", "iso2", "capital", "id", "city_ascii")
      .withColumnRenamed("iso3", "country_code")
      .withColumnRenamed("admin_name", "state")
      .withColumn("population", col("population").cast("bigint"))
      .orderBy("population")
      .na.drop()




    val myCountDf = df.filter(lower(col("country")).contains("india"))

    myCountDf.show()


    //==================================================

    println("=====|Minimum population in a state |=====")
    val minDf = myCountDf.groupBy("state")
      .agg(Map("population" -> "min"))
      .withColumnRenamed("min(Population)", "population")
      .orderBy("population")



    val minDfwithRank = minDf.withColumn("rank", monotonically_increasing_id() + 1).limit(10)
    val finalMinDf = minDfwithRank.select("rank", "state", "population")
    finalMinDf.show()

    finalMinDf.coalesce(1).write
      .mode("overwrite")
      .format("parquet").save("src/datasource/outputdataset/maxPop")


    //    ========================================

    println("=====|Maximum population |=====")
    val maxDf = myCountDf.groupBy("state")
      .agg(Map("population" -> "max"))
      .withColumnRenamed("max(Population)", "population")
      .orderBy(desc("population"))


    val maxDfwithRank = maxDf.withColumn("rank", monotonically_increasing_id() + 1).limit(10)
    val finalMaxDf = maxDfwithRank.select("rank", "state", "population")
finalMaxDf.show()


    finalMaxDf.coalesce(1).write
      .mode("overwrite")
      .format("parquet").save("src/datasource/outputdataset/maxPop")


    val sumDf = myCountDf.groupBy("state")
      .agg(Map("population" -> "sum"))
      .withColumnRenamed("sum(Population)", "population")
      .orderBy(desc("population"))

    val sumDfwithRank = maxDf.withColumn("rank", monotonically_increasing_id() + 1)
    val finalSumDf = maxDfwithRank.select("rank", "state", "population")
    println("=====|Total population |=====")
    finalSumDf.show()



    finalSumDf.coalesce(1).write
          .mode("overwrite")
          .format("parquet").save("src/datasource/outputdataset/sumPop")








  }

}