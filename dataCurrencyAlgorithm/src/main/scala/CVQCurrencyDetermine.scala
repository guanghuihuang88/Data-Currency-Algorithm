import CurrencyRepair.nowDate
import org.apache.spark.sql.{Dataset, SparkSession, functions, types}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ByteType, ShortType, StringType, StructType}

/**
 * CVQ时效性判定
 */
object CVQCurrencyDetermine {
  case class Student(eId: Short, vId: String, name: Byte, age: Byte, city: Byte, grade: Byte, status: Byte)
  def main(args: Array[String]): Unit = {
    val startTime: String = nowDate
        val spark: SparkSession = SparkSession.builder().appName("CurrencyDetermine").master("local[*]").getOrCreate()
//    val spark: SparkSession = SparkSession.builder().appName("CurrencyDetermine").master("spark://master:7077").getOrCreate()       // 集群的域名地址

    val structType = StructType(Array(
      types.StructField("eId", ShortType, false),
      types.StructField("vId", StringType, false),
      types.StructField("name", ByteType, false),
      types.StructField("age", ByteType, false),
      types.StructField("city", ByteType, false),
      types.StructField("grade", ByteType, false),
      types.StructField("status", ByteType, false),
    ))

    import spark.implicits._
    // 从文件中获取数据
    val dataSet: Dataset[Student] = spark.read.schema(structType).option("header", true).option("sep", ",").csv("src/main/resources/student.csv").as[Student]
//    val dataSet: Dataset[Student] = spark.read.schema(structType).option("header", true).option("sep", ",").csv("hdfs://master:9000/huangguanghui/data5G.csv").as[Student]
    dataSet.createTempView("student")

    // 计算cvq单个实体单个属性的时效性
    spark.udf.register("statusCvqUdaf", functions.udaf(StatusCvqUdaf))
    // 计算cvq2个属性的时效性
    val cvqUdf = udf(
      (value1: Double, value2: Double) => {
        (value1 + value2) / 2
      }
    )
    spark.udf.register("cvqUdf", cvqUdf)


    dataSet.first()

    val SQL1StartTime: String = nowDate
    // SQL1:计算特定实体e在一个属性A上的时效性,属性A的规则数为10
    val cvqTable1 = spark.sql("SELECT statusCvqUdaf(vId, grade, 'grade') as gradeCvq FROM student WHERE eId = '1'")
    cvqTable1.createTempView("cvqTable1")
    cvqTable1.show(1)

    val SQL2StartTime: String = nowDate
    // SQL2:计算特定实体e在属性A和属性B上的时效性,属性A的规则数为10,属性B的规则数为10
    val cvqTable2 = spark.sql("SELECT statusCvqUdaf(vId, grade, 'grade') as gradeCvq, statusCvqUdaf(vId, status, 'status') as statusCvq FROM student WHERE eId = '1'")
    cvqTable2.createTempView("cvqTable2")
    val cvq2 = spark.sql("SELECT cvqUdf(statusCvq, gradeCvq) as cvq FROM cvqTable2")
    cvq2.show(1)

    val SQL3StartTime: String = nowDate
    // SQL3:计算多个实体在属性A和属性B上的时效性,属性A的规则数为10,属性B的规则数为10
    val cvqTable3 = spark.sql("SELECT eId, statusCvqUdaf(vId, grade, 'grade') as gradeCvq, statusCvqUdaf(vId, status, 'status') as statusCvq FROM student WHERE eId like '1_' GROUP BY eId")
    cvqTable3.createTempView("cvqTable3")
    val cvq3 = spark.sql("SELECT avg(cvq) as avg FROM (SELECT eId, cvqUdf(statusCvq, gradeCvq) as cvq FROM cvqTable3)")
    cvq3.show(1)

    val SQL4StartTime: String = nowDate
    // SQL4:计算整个数据库的时效性,属性A的规则数为10,属性B的规则数为10
    val cvqTable4 = spark.sql("SELECT eId, statusCvqUdaf(vId, grade, 'grade') as gradeCvq, statusCvqUdaf(vId, status, 'status') as statusCvq FROM student GROUP BY eId")
    cvqTable4.createTempView("cvqTable4")
    val cvq4 = spark.sql("SELECT avg(cvq) as avg FROM (SELECT eId, cvqUdf(statusCvq, gradeCvq) as cvq FROM cvqTable4)")
    cvq4.show(1)

    val endTime: String = nowDate

    println("算法开始时间：" + startTime)
    println("SQL1开始时间：" + SQL1StartTime)
    println("SQL2开始时间：" + SQL2StartTime)
    println("SQL3开始时间：" + SQL3StartTime)
    println("SQL4开始时间：" + SQL4StartTime)
    println("算法结束时间：" + endTime)
    spark.stop()
  }
}


/**
 * 实验计划
 * 1. 不同数据量执行速度
 * 2. 不同维度sql执行速度
 * 3. 不同规则数执行速度
 */
