import java.io.FileWriter

import org.apache.spark.sql.SparkSession

object nitin_sangar_task2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("")
      .getOrCreate()

    val reviewPath = args(0)
    val businessPath = args(1)
    val outputTextFile = args(2)
    val outputJsonFile = args(3)
    val reviewData = spark.read.json(reviewPath).rdd
    val businessData = spark.read.json(businessPath).rdd
    val businessStars = reviewData.map(x => (x.getAs[String]("business_id"), x.getAs[Double]("stars")))
    val businessState = businessData.map(x => (x.getAs[String]("business_id"), x.getAs[String]("state")))
    val stateStars = businessStars.join(businessState).map(x => (x._2._2, x._2._1))
    val countRdd = stateStars.countByKey();
    val sumRdd = stateStars.reduceByKey((x, y) => x + y)
    val sortedAvg = sumRdd.map(x => (x._1, (x._2 / countRdd.get(x._1).get))).sortBy(x => (-x._2, x._1))

    var file = new FileWriter(outputTextFile)
    file.write("state,stars")

    for ((k, v) <- sortedAvg.collect()) {
      file.write("\n" + k + "," + v)
    }
    file.flush()

    val start = System.nanoTime()
    val avgList = sortedAvg.collect()
    for (i <- 0 to 4) {
      print(avgList(i) + "\n")
    }
    val timeFirst = System.nanoTime() - start

    val start2 = System.nanoTime()
    val avgList1 = sortedAvg.take(5)
    for ((k, v) <- avgList1) {
      print(k + "\n")
    }
    val timeSecond = System.nanoTime() - start2

    val file1 = new FileWriter(outputJsonFile)
    file1.write("{\n")
    file1.write("\"m1\": " + timeFirst + ",\n")
    file1.write("\"m2\": " + timeSecond + ",\n")
    file1.write("\"explanation\": " + "\"Collect() collects all the elements first so as to store them in the list and then we are printing the top 5 whereas in take() we are just taking the first five elements and not iterating over all the elements\"")
    file1.write("\n}")
    file1.flush()
  }
}
