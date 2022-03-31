import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import java.io.File


object SparkSQLTest {
   val spark = SparkSession.builder
  .appName("Simple Application")
  .master("local[2]")
     .enableHiveSupport()
     .getOrCreate()

  case class Person(id:Int, name:String, age:Int)

  def run(sparkSession: SparkSession): Unit =
  {
    import sparkSession.implicits._
    //接收文件,是rdd类型

    val rdd: RDD[String] = sparkSession.sparkContext.textFile("data/person.txt")
    //根据逗号进行分割，将字段赋值给people
    val df: DataFrame = rdd.map(_.split(",")).map(x => Person(x(0).toInt,x(1).toString, x(2).toInt)).toDF()
      //直接todf
    df.show()

    df.select(col("id"), col("name"), col("age") + 1).show

    df.createOrReplaceTempView("t_person")

    spark.sql("select * from t_person order by age desc limit 2").show

    spark.sql("select * from t_person where age > 30 ").show

    spark.sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")

    spark.sql("LOAD DATA LOCAL INPATH '/person.txt' INTO TABLE person")

    spark.sql("select * from person ").show()

    spark.stop()

  }

  def main(args: Array[String]) {
  run(spark)
  }
}
