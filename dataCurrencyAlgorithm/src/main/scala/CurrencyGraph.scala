import CurrencyGraph.Vertex
import org.apache.spark.sql.types.ByteType

import scala.collection.mutable.ListBuffer
import scala.collection.{Set, mutable}

object CurrencyGraph extends Serializable {

  class Vertex extends Serializable {
    var columnValue: String = _
    var entityTuples: Set[String] = _
    var inDegree: Int = _
    var inVertexes: mutable.HashSet[String] = _
    var outDegree: Int = _
    var outVertexes: mutable.HashSet[String] = _
    // CVQ算法的辅助属性
    var entityNum: Int = _
    // CSQ算法的辅助属性
    var preNode: Vertex = _
    var len: Int = 0
    // 修复算法的辅助属性
    var topoSortNum: Byte = 0

    def this(columnValue: String) {
      this()
      this.columnValue = columnValue
      this.entityTuples = Set[String]()
      inDegree = 0
      inVertexes = mutable.HashSet[String]()
      outDegree = 0
      outVertexes = mutable.HashSet[String]()
    }
  }

}

class CurrencyGraph extends Serializable {

  // key：属性名 value：时效图顶点
  val graph: mutable.HashMap[String, Vertex] = new mutable.HashMap[String, Vertex]()

  // 时效图对应的时效约束
  var rule: List[Tuple2[String, String]] = List[Tuple2[String, String]]()

  // 判定算法♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥
  // 添加记录(用于判定算法）
  def addAsDistinct(vId: String, statusValue: String): CurrencyGraph = {
    val vertex = this.graph.get(statusValue)
    vertex match {
      case Some(vertex) => {
        vertex.entityNum += 1
        if(vertex.entityTuples.isEmpty) {
          vertex.entityTuples += vId
        }
      }
      case None => {
        val newVertex: Vertex = new Vertex(statusValue)
        this.graph.put(statusValue, newVertex)
        newVertex.entityTuples += vId
        newVertex.entityNum += 1
      }
    }
    this
  }
  // 时效图的合并
  def +(currencyGraph: CurrencyGraph): CurrencyGraph = {
    val valuesSet: Iterable[Vertex] = currencyGraph.graph.values

    valuesSet.foreach(value =>
    {
      val vertex: Vertex = this.graph.getOrElse(value.columnValue,
        {
          val vertex: Vertex = new Vertex(value.columnValue)
          this.graph.put(value.columnValue, vertex)
          vertex
        }
      )
      if(vertex.entityTuples.isEmpty) {
        vertex.entityTuples = vertex.entityTuples.union(value.entityTuples)
      }
      vertex.entityNum += value.entityNum
    }
    )

    this
  }
  // CVQ时效性判定算法
  def computeCVQ: Double = {
    var minEntityNum: Int = Int.MaxValue
    var sumEntityNum: Int = 0
    if(this.graph.nonEmpty){
      val vertexes = this.graph.values
      vertexes.foreach(v =>
        {
          if(v.outDegree == 0) {
            minEntityNum = Math.min(v.entityNum, minEntityNum)
            sumEntityNum += v.entityNum
          }
        }
      )
    }
    minEntityNum.toDouble / sumEntityNum.toDouble
  }

  // HSQ时效性判定算法
  def computeHSQ: Double = {
    var maxLenVertex: Vertex = new Vertex()
    val sq: ListBuffer[String] = ListBuffer[String]()

    val que = new mutable.Queue[Vertex]
    val vertexes: List[Vertex] = this.graph.values.toList
    for(vertex <- vertexes) {
      if(vertex.inDegree == 0) {
        vertex.len = 1
        que += vertex
      }
    }

    while(que.nonEmpty) {
      val front: Vertex = que.front
      que.dequeue()
      for (outVertexName <- front.outVertexes) {
        val outVertex = this.graph.get(outVertexName)
        outVertex match {
          case Some(outVertex) => {
            outVertex.len = front.len + 1
            if(outVertex.len > maxLenVertex.len) {
              maxLenVertex = outVertex
            }
            outVertex.preNode = front
            que.enqueue(outVertex)
          }
          case None => {}
        }
      }
    }
    while (maxLenVertex != null) {
      sq += maxLenVertex.columnValue
      maxLenVertex = maxLenVertex.preNode
    }
    sq.size.toDouble / this.graph.size.toDouble
  }


  // 修复算法♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥
  // 添加记录(用于修复算法）
  def add(vId: String, statusValue: String): CurrencyGraph = {
    val vertex = this.graph.get(statusValue)
    vertex match {
      case Some(vertex) => {
          vertex.entityTuples += vId
      }
      case None => {
        val newVertex: Vertex = new Vertex(statusValue)
        this.graph.put(statusValue, newVertex)
        newVertex.entityTuples += vId
      }
    }
    this
  }


  // 时效图的顶点添加拓扑序
  def toposort = {
    var orderID: Int = 1
    val que = new mutable.Queue[Vertex]

    for(vertex <- this.graph.values) {
      if(vertex.inDegree == 0) {
        que += vertex
      }
    }

    while(que.nonEmpty) {
      val front: Vertex = que.front
      que.dequeue()
      front.topoSortNum = orderID.toByte
      orderID = orderID + 1
      for (outVertexName <- front.outVertexes) {
        val outVertex = this.graph.get(outVertexName)
        outVertex match {
          case Some(outVertex) => {
            front.outDegree -= 1
            outVertex.inDegree -= 1
            if(outVertex.inDegree == 0) {
              que.enqueue(outVertex)
            }
          }
          case None => {}
        }
      }
    }
  }
  def topoSortNum(value: Byte): Byte = {
    val outVertex = this.graph.get(value.toString)
    outVertex match {
      case Some(outVertex) => {
        outVertex.topoSortNum
      }
      case None => {
        0.toByte
      }
    }
  }

  // 时效图的拓扑排序（row_number()型编号）
  def toposort1: mutable.HashMap[String, Int] = {
    val result: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
    var orderID: Int = 1
    val que = new mutable.Queue[Vertex]
    val vertexes: List[Vertex] = this.graph.values.toList
    for(vertex <- vertexes) {
      if(vertex.inDegree == 0) {
        vertex.len = 1
        que += vertex
      }
    }

    while(que.nonEmpty) {
      val front: Vertex = que.front
      que.dequeue()
      front.entityTuples.foreach(
        tupleID => {
          result.put(tupleID, orderID)
          orderID += 1
        }
      )
      for (outVertexName <- front.outVertexes) {
        val outVertex = this.graph.get(outVertexName)
        outVertex match {
          case Some(outVertex) => {
            front.outDegree -= 1
            outVertex.inDegree -= 1
            if(outVertex.inDegree == 0) {
              que.enqueue(outVertex)
            }
          }
          case None => {}
        }
      }
    }
    result
  }

  // 时效图的拓扑排序（dense_rank()型编号）
  def toposort2: mutable.HashMap[String, Int] = {
    val result: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
    var orderID: Int = 0
    val que = new mutable.Queue[Vertex]
    val vertexes: List[Vertex] = this.graph.values.toList
    for(vertex <- vertexes) {
      if(vertex.inDegree == 0) {
        vertex.len = 1
        que += vertex
      }
    }

    while(que.nonEmpty) {
      val front: Vertex = que.front
      que.dequeue()
      orderID += 1
      front.entityTuples.foreach(
        tupleID => {
          result.put(tupleID, orderID)
        }
      )
      for (outVertexName <- front.outVertexes) {
        val outVertex = this.graph.get(outVertexName)
        outVertex match {
          case Some(outVertex) => {
            front.outDegree -= 1
            outVertex.inDegree -= 1
            if(outVertex.inDegree == 0) {
              que.enqueue(outVertex)
            }
          }
          case None => {}
        }
      }
    }
    result
  }

  // 时效数据的错误检测算法
  def compare(columnValue1: String, columnValue2: String): String = {
    if(columnValue1 == columnValue2)
      return "<>"
    val Vertex1 = this.graph.getOrElse(columnValue1, new Vertex(columnValue1))
    val Vertex2 = this.graph.getOrElse(columnValue2, new Vertex(columnValue2))

    if(bfs(Vertex1, Vertex2)){
      return "<"
    }
    else if(bfs(Vertex2, Vertex1)){
      return ">"
    }
    else
      return "<>"
  }
  // 广度搜索：是否有一条从vertex1到vertex2的有向边
  def bfs(vertex1: Vertex, vertex2: Vertex): Boolean = {
    if(vertex1 == vertex2)
      return true

    var result: Boolean = false
    for(outVertexName <- vertex1.outVertexes){
      val outVertex = this.graph.getOrElse(outVertexName, new Vertex(outVertexName))
      result = result || bfs(outVertex, vertex2)
    }
    result
  }


  // 时效图的过滤♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥
  def fresh: CurrencyGraph = {
    val que = new mutable.Queue[Vertex]
    val vertexes: List[Vertex] = this.graph.values.toList
    for(vertex <- vertexes) {
      if(vertex.inDegree == 0) {
        que += vertex
      }
    }
    while(que.nonEmpty) {
      val front: Vertex = que.front
      que.dequeue()
      // 后继顶点添加到que
      for(outVertexName <- front.outVertexes) {
        val outVertex = this.graph.get(outVertexName)
        outVertex match {
          case Some(outVertex) => {
            if(outVertex.inDegree == 0) {
              que.enqueue(outVertex)
            }
          }
          case None => {}
        }
      }
      // 若顶点内容为空
      if(front.entityTuples.isEmpty) {
        // 从时效图中删去空顶点
        this.graph.remove(front.columnValue)
        // 更新空顶点后继顶点的信息
        for (outVertexName <- front.outVertexes) {
          val outVertex = this.graph.get(outVertexName)
          outVertex match {
            case Some(outVertex) => {
              outVertex.inDegree -= 1
              outVertex.inVertexes.remove(front.columnValue)
            }
            case None => {}
          }
        }
        // 更新空顶点前驱顶点的信息
        for (inVertexName <- front.inVertexes) {
          val inVertex = this.graph.get(inVertexName)
          inVertex match {
            case Some(inVertex) => {
              inVertex.outDegree -= 1
              inVertex.outVertexes.remove(front.columnValue)

              for (outVertexName <- front.outVertexes) {
                val outVertex = this.graph.get(outVertexName)
                outVertex match {
                  case Some(outVertex) => {
                    // 添加前驱顶点到后继顶点的边
                    inVertex.outDegree += 1
                    inVertex.outVertexes.add(outVertexName)
                    outVertex.inDegree += 1
                    outVertex.inVertexes.add(inVertexName)
                  }
                  case None => {}
                }
              }

            }
            case None => {}
          }

        }
      }
    }

    this
  }


}
