package training

import org.apache.spark.sql.SparkSession

object ReadTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Read")
      .master("local[1]")
      .getOrCreate()

    val reviewers = spark.read.json("data/reviewers_small.json")

    reviewers.printSchema()

    reviewers.show(100)

    spark.stop()
  }
}

