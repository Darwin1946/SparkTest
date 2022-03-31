
  /* SimpleApp.scala */
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

  import java.io.File

  object SimpleApp {
    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      while (iter.hasNext) {
        val cur = iter.next
        res .::= (pre, cur)
        pre = cur;
      }
      res.iterator
    }

    def main(args: Array[String]) {
      val file = new File("")
      val project_path = file.getAbsolutePath()
      val logFile = project_path+"/data/JaneEyre.txt"
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      println(logData.count())
      println(logData.first())
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

      val a = sc.parallelize(1 to 9, 3)
      val b = a.mapPartitions(myfunc).foreach(println)

    }
  }

