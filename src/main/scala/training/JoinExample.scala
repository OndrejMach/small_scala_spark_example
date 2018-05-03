package training

import org.apache.spark.sql.SparkSession

object JoinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Read")
      .master("local[1]")
      .getOrCreate()

    val reviewers = spark.read.json("data/reviewers_small.json")
    reviewers.printSchema()
    reviewers.show(100)

    val reviews = spark.read.json("data/reviews_small.json")
    reviews.printSchema()
    reviews.show(100)

    val joined = reviewers.join(reviews, reviewers("id") === reviews("reviewerID"))

    joined.printSchema()
    joined.show()

    //show top 10 most active reviewers

  }
}
