import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.reflect.runtime.{universe => ru}
import scala.util.control._
import org.apache.spark.sql.functions.lit

import scala.collection.mutable

/***
 * 这是没有进行内存优化的Repair算法
 * 1. 数据结构用的都是String
 * 2. 排序也是论文中的实现方式
 * 3. repair的groupBy没有压缩数据结构
 */
object CurrencyRepair {

  case class Record(var eId: String, var vId: String, var name: String, var age: String, var city: String, var grade: String, var status: String)

  case class GroupedRecord(var groupId: Int, var eId: String, var vId: String, var name: String, var age: String, var city: String, var grade: String, var status: String)

  case class SortedRecord(var groupId: Int, var sortId: Int, var eId: String, var vId: String, var name: String, var age: String, var city: String, var grade: String, var status: String)

  def record2GroupedRecord(groupId: Int, record: Record): GroupedRecord = {
    GroupedRecord(groupId, record.eId, record.vId, record.name, record.age, record.city, record.grade, record.status)
  }

  def groupedRecord2SortedRecord(sortId: Int, record: GroupedRecord): SortedRecord = {
    SortedRecord(record.groupId, sortId, record.eId, record.vId, record.name, record.age, record.city, record.grade, record.status)
  }

  def SortedRecord2Record(record: SortedRecord): Record = {
    Record(record.eId, record.vId, record.name, record.age, record.city, record.grade, record.status)
  }

  /***
   * 时效性错误修复算法
   * 1. 两趟排序算法
   * 2. 修复算法
   */
  def main(args: Array[String]): Unit = {

    val startTime: String = nowDate

//    val spark: SparkSession = SparkSession.builder().appName("RepairAlgorithm").master("local[*]").getOrCreate()
    val spark: SparkSession = SparkSession.builder().appName("CurrencyRepair").master("spark://master:7077").getOrCreate()
    import spark.implicits._

    // 从文件中获取数据
//    val dataFrame: DataFrame = spark.read.option("header", true).option("sep", ",").csv("D:\\研二下学期\\data\\studentForRepair.csv")
    val dataFrame: DataFrame = spark.read.option("header", true).option("sep", ",").csv("hdfs://master:9000/huangguanghui/data5MMM.csv")
    val rdd: RDD[Record] = dataFrame.as[Record].rdd

    val sortStartTime: String = nowDate
    // 第一趟排序
    val groupedRDD: RDD[GroupedRecord] = rdd.groupBy(record => record.eId).mapValues(
      iter => {
        firstSortForColumn(iter, "grade")
      }
    ).values.flatMap(x => x)

    // 第二趟排序
    val sortedRDD: RDD[SortedRecord] = groupedRDD.groupBy(record => (record.eId, record.groupId)).mapValues(
      iter => {
        iter.to
        secondSortForColumn(iter, "status")
      }
    ).values.flatMap(x => x)

    sortedRDD.first()
    // 修复
    val repairStartTime: String = nowDate
    val repairRDD: RDD[Record] = sortedRDD.groupBy(record => record.eId).map(
      tuple2 => {
        val arr: Array[SortedRecord] = tuple2._2.toArray
        repairForColumn(arr, "grade", "status")
      }
    ).flatMap(x => x)

    val result: DataFrame = repairRDD.toDF("eId", "vId", "name", "age", "city", "grade", "status")

//    result.coalesce(1).write.option("header", "true").option("sep", ",").csv("hdfs://master:9000/huangguanghui/out/dataMMM.csv")
    result.coalesce(100).write.format("csv").mode("overwrite").option("header", "true").option("encodeing", "utf-8").save(s"hdfs://master:9000/huangguanghui/out/data5MMM.csv")

    val endTime: String = nowDate

    println("startTime：" + startTime)
    println("sortStartTime：" + sortStartTime)
    println("repairStartTime：" + repairStartTime)
    println("endTime：" + endTime)

    spark.stop()
  }

  // 第一趟排序：根据referenceColumn排序
  def firstSortForColumn(iter: Iterable[Record], referenceColumn: String): Array[GroupedRecord] = {
    // 通过rdd构造时效图
    var currencyGraph: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph(referenceColumn)
    iter.foreach(
      record => {
        // 通过反射获取record指定属性名上的属性值
        val mirrOfRecord = ru.runtimeMirror(record.getClass.getClassLoader)
        val vId = mirrOfRecord.reflect(record).reflectField(ru.typeOf[Record].decl(ru.TermName("vId")).asTerm).get.toString
        val referenceColumnValue = mirrOfRecord.reflect(record).reflectField(ru.typeOf[Record].decl(ru.TermName(referenceColumn)).asTerm).get.toString
        currencyGraph.add(vId, referenceColumnValue)
      }
    )
    currencyGraph.fresh
    // 通过时效图进行topo排序
    val map: mutable.HashMap[String, Int] = currencyGraph.toposort2
    val arrayBuffer: mutable.ArrayBuffer[GroupedRecord] = mutable.ArrayBuffer[GroupedRecord]()
    iter.foreach(
      record => {
        arrayBuffer += record2GroupedRecord(map.getOrElse(record.vId, 0), record)
        record
      }
    )

    // 垃圾回收
    currencyGraph = null
    System.gc()

    arrayBuffer.toArray
  }

  // 第二趟排序：根据referenceColumn分组，根据repairColumn排序
  def secondSortForColumn(iter: Iterable[GroupedRecord], repairColumn: String): Array[SortedRecord] = {
    // 通过rdd构造时效图
    var currencyGraph: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph(repairColumn)
    iter.foreach(
      record => {
        // 通过反射获取record指定属性名上的属性值
        val mirrOfRecord = ru.runtimeMirror(record.getClass.getClassLoader)
        val vId = mirrOfRecord.reflect(record).reflectField(ru.typeOf[GroupedRecord].decl(ru.TermName("vId")).asTerm).get.toString
        val repairColumnValue = mirrOfRecord.reflect(record).reflectField(ru.typeOf[GroupedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString
        currencyGraph.add(vId, repairColumnValue)
      }
    )
    currencyGraph.fresh
    // 通过时效图进行topo排序
    val map: mutable.HashMap[String, Int] = currencyGraph.toposort1
    val arrayBuffer: mutable.ArrayBuffer[SortedRecord] = mutable.ArrayBuffer[SortedRecord]()
    iter.foreach(
      record => {
        arrayBuffer += groupedRecord2SortedRecord(map.getOrElse(record.vId, 0), record)
      }
    )

    // 垃圾回收
    currencyGraph = null
    System.gc()

    arrayBuffer.toArray
  }


  // 时效修复算法：基于referenceColumn列上的时效约束修复repairColumn列上的数据
  def repairForColumn(arr: Array[SortedRecord], referenceColumn: String, repairColumn: String): Array[Record] = {
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
      val mirrOfRow1 = ru.runtimeMirror(row1.getClass.getClassLoader)
      val row1ReferenceColumn = mirrOfRow1.reflect(row1).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(referenceColumn)).asTerm).get.toString
      val row1RepairColumn = mirrOfRow1.reflect(row1).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString
      val mirrOfRow2 = ru.runtimeMirror(row2.getClass.getClassLoader)
      val row2ReferenceColumn = mirrOfRow2.reflect(row2).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(referenceColumn)).asTerm).get.toString
      val row2RepairColumn = mirrOfRow2.reflect(row2).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString

      /***
       * 当 row1RepairColumn < 或 <> row2RepairColumn 时，无需进行修复，只需延长或更新滑动窗口
       * 1. 当 row1RepairColumn <> row2RepairColumn 时：延长滑动窗口
       * 2. 当 row1RepairColumn < row2RepairColumn 时：
       *    1. 当窗口内所有 rowRepairColumn < row2RepairColumn 时：更新滑动窗口
       *    2. 否则：延长滑动窗口
       */
      if (repairGraph.compare(row1RepairColumn, row2RepairColumn) == "<>") {
        right += 1
      }
      else if (repairGraph.compare(row1RepairColumn, row2RepairColumn) == "<") {

        var flag: Boolean = false

        /***
         * 1. 若row2的值 > 滑动窗口中的所有属性值，则更新滑动窗口
         * 2. 若row2的值 <> 滑动窗口中的任一属性值，则延长滑动窗口
         * 3. 根据有向无环图的属性易知，row2的值 > 滑动窗口中的任一属性值 的情况不存在
         */
        for (curs <- left until right) {
          // 通过反射获取curs位置的值
          val rowCurs: SortedRecord = sortedArr(curs)
          val rowCursRepairColumn = ru.runtimeMirror(rowCurs.getClass.getClassLoader).reflect(rowCurs).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString
          if (repairGraph.compare(rowCursRepairColumn, row2RepairColumn) == "<>") {
            flag = true
          }
        }

        // “向后多看一眼”，若出现需要修复的情况，则延长滑动窗口，待修复之后再做判断
        if (i < sortedArr.length-2) {
          val row3: SortedRecord = sortedArr(i + 2)
          val row3RepairColumn = ru.runtimeMirror(row3.getClass.getClassLoader).reflect(row3).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString

          if(repairGraph.compare(row2RepairColumn, row3RepairColumn) == ">") {
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
      else if(repairGraph.compare(row1RepairColumn, row2RepairColumn) == ">") {

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
            val rowCursRepairColumn = ru.runtimeMirror(rowCurs.getClass.getClassLoader).reflect(rowCurs).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString
            if(repairGraph.compare(rowCursRepairColumn, row2RepairColumn) == ">") {
              flag = true
              // 循环中断
              loop.break
            }
          }
        }

        if (flag) {
          // 通过反射修复row2的数据：用row1RepairColumn替换row2RepairColumn的值
          mirrOfRow2.reflect(row2).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).set(row1RepairColumn)
          // 延长滑动窗口
          right += 1
        }
        else {
          // 通过反射修复row1的数据：用row2RepairColumn替换row1RepairColumn的值
          mirrOfRow1.reflect(row1).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).set(row2RepairColumn)

          for (curs <- left until right) {
            // 通过反射获取curs位置的值
            val rowCurs: SortedRecord = sortedArr(curs)
            val rowCursRepairColumn = ru.runtimeMirror(rowCurs.getClass.getClassLoader).reflect(rowCurs).reflectField(ru.typeOf[SortedRecord].decl(ru.TermName(repairColumn)).asTerm).get.toString
            if (repairGraph.compare(rowCursRepairColumn, row1RepairColumn) == "<>") {
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

    val arrayBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer[Record]()
    arr.foreach(
      record => {
        arrayBuffer += SortedRecord2Record(record)
      }
    )
    System.gc()

    arrayBuffer.toArray
  }


  def nowDate: String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }
}
