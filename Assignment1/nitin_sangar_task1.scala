import java.io.FileWriter
import org.apache.spark.sql.{Row, SparkSession}

object nitin_sangar_task1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)


    val sc = spark.sparkContext;

    val data = spark.read.json(inputPath).rdd

    // Useful reviews
    val usefulReviews = data.filter(x => (x.getAs[Long]("useful") > 0)).count()

    val fiveStarsCount = data.filter(x => x.getAs[Double]("stars") == 5).count()

    val maxReview = data.map(x => x.getAs[String]("text").length()).max();

    val distinctUsers = data.map(x => x.getAs[String]("user_id")).distinct().count();

    val users = data.map(x => (x.getAs[String]("user_id"), 1))
    val userRdd = users.countByKey()
    val sortedUsers = userRdd.toSeq.sortWith { (x, y) => (x._2 > y._2) || ((x._2 == y._2) && x._1 < y._1) }.take(20).toList

    val distinctBusiness = data.map(x => x.getAs[String]("business_id")).distinct().count();

    val business = data.map(x => (x.getAs[String]("business_id"), 1))
    val businessRdd = business.countByKey()
    val sortedBusiness = businessRdd.toSeq.sortWith { (x, y) => (x._2 > y._2) || ((x._2 == y._2) && x._1 < y._1) }.take(20).toList

    val file = new FileWriter(outputPath)
    file.write("{")
    file.write("\n\"n_review_useful\": " + usefulReviews + ",")
    file.write("\n\"n_review_5_star\": " + fiveStarsCount + ",")
    file.write("\n\"n_characters\": " + maxReview + ",")
    file.write("\n\"n_user\": " + distinctUsers + ",")
    file.write("\n\"top20_user\": [");
    var i = 1
    for ((k, v) <- sortedUsers) {
      file.write("[")
      file.write("\"" + k.toString() + "\"")
      file.write("," + v)
      i += 1
      if (i == 21) {
        file.write("]")
      } else {
        file.write("],")
      }
    }
    file.write("],")
    file.write("\n\"n_business\": " + distinctBusiness + ",")
    file.write("\n\"top20_business\": [");
    i = 1
    for ((k, v) <- sortedBusiness) {
      file.write("[")
      file.write("\"" + k.toString() + "\"")
      file.write("," + v)
      i += 1
      if (i == 21) {
        file.write("]")
      } else {
        file.write("],")
      }
    }
    file.write("]")
    file.write("\n}")
    file.flush()
  }
}
