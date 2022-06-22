import scala.util.control.Breaks
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types.{ByteType, ShortType, StringType, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders.STRING
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

import scala.reflect.runtime.{universe => ru}
import scala.collection.mutable

/***
 * 针对内存进行过优化的Repair算法
 * 1. 数据结构中的属性选用Byte、Short等类型
 * 2. 排序算法用map实现
 * 3. repair的groupBy对记录进行了“压缩”
 */
object optimizedCurrencyRepair {

  case class Record(var eId: Short, var vId: String, var name: Byte, var age: Byte, var city: Byte, var grade: Byte, var status: Byte)
  case class SortedRecord(var groupId: Byte, var sortId: Byte, var eId: Short, var vId: String, var grade: Byte, var status: Byte)


  def record2SortedRecord(groupId: Byte, sortId: Byte, record: Record): SortedRecord = {
    SortedRecord(groupId, sortId, record.eId, record.vId, record.grade, record.status)
  }

  /** *
   * 时效性错误修复算法
   * 1. 两趟排序算法
   * 2. 修复算法
   */
  def main(args: Array[String]): Unit = {

    val startTime: String = nowDate

//    val spark: SparkSession = SparkSession.builder().appName("RepairAlgorithm").master("local[*]").getOrCreate()
    val spark: SparkSession = SparkSession.builder().appName("CurrencyRepair").master("spark://master:7077").getOrCreate()
    import spark.implicits._

    val structType = StructType(Array(
      types.StructField("eId", ShortType, false),
      types.StructField("vId", StringType, false),
      types.StructField("name", ByteType, false),
      types.StructField("age", ByteType, false),
      types.StructField("city", ByteType, false),
      types.StructField("grade", ByteType, false),
      types.StructField("status", ByteType, false),
    ))

    // 从文件中获取数据

//    val dataFrame: DataFrame = spark.read.schema(structType).option("header", true).option("sep", ",").csv("D:\\研二下学期\\data\\data1G.csv")
    val dataFrame: DataFrame = spark.read.schema(structType).option("header", true).option("sep", ",").csv("hdfs://master:9000/huangguanghui/data1G.csv")

    val rdd: RDD[Record] = dataFrame.as[Record].rdd

    val sortStartTime: String = nowDate
    // 排序
    var referenceColumnGraph: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph("grade")
    var repairColumnGraph: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph("status")
    val sortedRDD: RDD[SortedRecord] = rdd.map(
      record => {
        val firstSortNum: Byte = referenceColumnGraph.topoSortNum(record.grade)
        val secondSortNum: Byte = repairColumnGraph.topoSortNum(record.status)
        record2SortedRecord(firstSortNum, secondSortNum, record)
      }
    )
    sortedRDD.first()

    // 修复
    val repairStartTime: String = nowDate
    val repairRDD: RDD[(String, Byte)] = sortedRDD.groupBy(record => record.eId).map(
      tuple2 => {
        val iterator = tuple2._2.iterator
        val arrayBuffer: mutable.ArrayBuffer[SortedRecord] = mutable.ArrayBuffer[SortedRecord]()
        var preRecord: SortedRecord = iterator.next()
        arrayBuffer += preRecord
        while (iterator.hasNext) {
          val tmp: SortedRecord = iterator.next()
          if(tmp.status != preRecord.status || tmp.grade != preRecord.grade) {
            arrayBuffer += tmp
          }
          preRecord = tmp
        }

        repairForColumn(arrayBuffer.toArray, "status")

//        val arrayBuffer: mutable.ArrayBuffer[(String, Byte)] = mutable.ArrayBuffer[(String, Byte)]()
//        arr.foreach(
//          record => {
//            arrayBuffer += Tuple2(record.vId, record.status)
//          }
//        )
//
//        arrayBuffer.toArray
        tuple2._2.toArray
      }
    ).flatMap(x => x).map(
      record => {
        Tuple2(record.vId, record.status)
      }
    )

    val tmpDF: DataFrame = dataFrame.select("eId", "vId", "name", "age", "city", "grade")
    val repairDF: DataFrame = repairRDD.toDF("_vId", "status")
    val result: DataFrame = tmpDF.join(repairDF, tmpDF("vId") === repairDF("_vId"), "inner").drop("_vId")

    result.coalesce(100).write.format("csv").mode("overwrite").option("header", "true").option("encodeing", "utf-8").save(s"hdfs://master:9000/huangguanghui/out/data1G.csv")
//    result.coalesce(100).write.format("csv").mode("overwrite").option("header", "true").option("encodeing", "utf-8").save("D:\\研二下学期\\data\\out\\data1G.csv")

    val endTime: String = nowDate

    println("startTime：" + startTime)
    println("sortStartTime：" + sortStartTime)
    println("repairStartTime：" + repairStartTime)
    println("endTime：" + endTime)


    spark.stop()
  }

  // 时效修复算法：基于referenceColumn列上的时效约束修复repairColumn列上的数据
  def repairForColumn(arr: Array[SortedRecord], repairColumn: String): Unit = {
    // repairColumn列上的时效约束构造时效图
    val repairGraph: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph(repairColumn)

    val sortedArr: Array[SortedRecord] = arr.sortBy(record => (record.groupId, record.sortId))             // 按照(groupId,sortId)排序

    // 滑动窗口算法：left -> right
    var left: Int = 0
    var right: Int = 0
    for(i <- 0 until sortedArr.length-1) {

      val row1: SortedRecord = sortedArr(i)
      val row2: SortedRecord = sortedArr(i + 1)
      // 通过反射获取row1和row2指定属性名上的属性值
      val row1RepairColumn: Byte = row1.status
      val row2RepairColumn: Byte = row2.status
      /***
       * 当 row1RepairColumn < 或 <> row2RepairColumn 时，无需进行修复，只需延长或更新滑动窗口
       * 1. 当 row1RepairColumn <> row2RepairColumn 时：延长滑动窗口
       * 2. 当 row1RepairColumn < row2RepairColumn 时：
       *    1. 当窗口内所有 rowRepairColumn < row2RepairColumn 时：更新滑动窗口
       *    2. 否则：延长滑动窗口
       */
      if (repairGraph.compare(row1RepairColumn.toString, row2RepairColumn.toString) == "<>") {
        right += 1
      }
      else if (repairGraph.compare(row1RepairColumn.toString, row2RepairColumn.toString) == "<") {

        var flag: Boolean = false

        /***
         * 1. 若row2的值 > 滑动窗口中的所有属性值，则更新滑动窗口
         * 2. 若row2的值 <> 滑动窗口中的任一属性值，则延长滑动窗口
         * 3. 根据有向无环图的属性易知，row2的值 > 滑动窗口中的任一属性值 的情况不存在
         */
        for (curs <- left until right) {
          // 通过反射获取curs位置的值
          val rowCurs: SortedRecord = sortedArr(curs)
          val rowCursRepairColumn = rowCurs.status
          if (repairGraph.compare(rowCursRepairColumn.toString, row2RepairColumn.toString) == "<>") {
            flag = true
          }
        }

        // “向后多看一眼”，若出现需要修复的情况，则延长滑动窗口，待修复之后再做判断
        if (i < sortedArr.length-2) {
          val row3: SortedRecord = sortedArr(i + 2)
          val row3RepairColumn = row3.status

          if(repairGraph.compare(row2RepairColumn.toString, row3RepairColumn.toString) == ">") {
            flag = true
          }
        }

        if (flag) {
          // 延长滑动窗口
          right += 1
        }
        else {
          // 更新滑动窗口
          left = i+1
          right = i+1
        }
      }

      /***
       * 当 row1RepairColumn > row2RepairColumn 时，需要进行修复
       * 1. 当 row1ReferenceColumn < row2ReferenceColumn 时：
       *    1. 当 row2RepairColumn > 或 <> 窗口中除 row1RepairColumn 的其他所有值时： 将 row1RepairColumn 修改为 row2RepairColumn
       *    2. 否则，row2RepairColumn 修改为 row1RepairColumn
       * 2. 当 row1ReferenceColumn <> row2ReferenceColumn 时：将 row2 向上交换到合适位置
       */
      else if(repairGraph.compare(row1RepairColumn.toString, row2RepairColumn.toString) == ">") {

        //        if(referenceGraph.compare(row1ReferenceColumn, row2ReferenceColumn) == "<") {
        var flag: Boolean = false
        // 创建 Breaks 对象
        val loop = new Breaks
        // 在 breakable 中循环
        loop.breakable{
          // 循环
          for(curs <- left until right) {
            // 通过反射获取curs位置的值
            val rowCurs: SortedRecord = sortedArr(curs)
            val rowCursRepairColumn = rowCurs.status
            if(repairGraph.compare(rowCursRepairColumn.toString, row2RepairColumn.toString) == ">") {
              flag = true
              // 循环中断
              loop.break
            }
          }
        }

        if (flag) {
          // 通过反射修复row2的数据：用row1RepairColumn替换row2RepairColumn的值
          val mirrOfRow2 = ru.runtimeMirror(row2.getClass.getClassLoader)
          mirrOfRow2.reflect(row2).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).set(row1RepairColumn)
          // 延长滑动窗口
          right += 1
        }
        else {
          // 通过反射修复row1的数据：用row2RepairColumn替换row1RepairColumn的值
          val mirrOfRow1 = ru.runtimeMirror(row1.getClass.getClassLoader)
          mirrOfRow1.reflect(row1).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).set(row2RepairColumn)

          for (curs <- left until right) {
            // 通过反射获取curs位置的值
            val rowCurs: SortedRecord = sortedArr(curs)
            val rowCursRepairColumn = rowCurs.status
            if (repairGraph.compare(rowCursRepairColumn.toString, row1RepairColumn.toString) == "<>") {
              flag = true
            }
          }

          if (flag) {
            // 延长滑动窗口
            right += 1
          }
          else {
            // 更新滑动窗口
            left = i
            right = i
          }
        }


      }
    }
  }


  def nowDate: String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }
}
