import scala.collection.mutable

object StudentGraphsUtil extends Serializable {
  // Student数据的时效规则
  val ruleMap: mutable.HashMap[String, List[Tuple2[String, String]]] = mutable.HashMap[String, List[Tuple2[String, String]]](
    "eId" -> List[Tuple2[String, String]](),
    "vId" -> List[Tuple2[String, String]](),
    "name" -> List[Tuple2[String, String]](),
    "age" -> List[Tuple2[String, String]](("low", "high")),        // ("low", "high"):单调递增；("high", "low")：单调递减
    "city" -> List[Tuple2[String, String]](),
//    "grade" -> List[Tuple2[String, String]](("HighSchool", "Bachelor"), ("Bachelor", "Master"), ("Master", "PHD"), ("PHD", "Graduate")),
//    "status" -> List[Tuple2[String, String]](("Single", "Married"), ("Married", "Divorced"), ("Divorced", "Remarried"))
    "grade" -> List[Tuple2[String, String]](
      ("1", "3"), ("2", "3"), ("3", "4"), ("3", "6"),
      ("4", "5"), ("5", "7"), ("6", "7"), ("7", "8"),
      ("7", "9"), ("8", "10"), ("9", "10")
    ),
//    "grade" -> List[Tuple2[String, String]](
//      ("1", "4"), ("1", "5"), ("2", "5"), ("2", "6"), ("3", "6"), ("3", "7"), ("3", "8"), ("4", "9"), ("5", "10"), ("5", "12"), ("6", "9"), ("7", "10"), ("7", "11"), ("8", "19"),
//      ("9", "12"), ("10", "12"), ("10", "13"), ("11", "14"), ("12", "15"), ("12", "16"), ("13", "16"), ("13", "17"), ("14", "18"), ("14", "19"), ("15", "20"), ("16", "21"), ("16", "23"),
//      ("17", "20"), ("18", "21"), ("18", "22"), ("19", "25"), ("20", "23"), ("21", "24"), ("22", "23"), ("22", "25"), ("23", "27"), ("23", "29"), ("23", "30"), ("24", "26"), ("24", "28"),
//      ("25", "28"), ("26", "31"), ("27", "32"), ("27", "34"), ("28", "31"), ("29", "32"), ("29", "33"), ("30", "36"), ("31", "37"), ("32", "35"), ("33", "34"), ("33", "36"), ("34", "37"),
//      ("34", "40"), ("35", "38"), ("35", "41"), ("36", "38"), ("36", "40"), ("37", "42"), ("38", "43"), ("38", "45"), ("39", "42"), ("40", "43"), ("40", "44"), ("41", "47"), ("42", "48"),
//      ("43", "46"), ("44", "45"), ("44", "47"), ("45", "50"), ("46", "48"), ("47", "49"), ("48", "50"), ("49", "50")
//    ),
//    "grade" -> List[Tuple2[String, String]](
//      ("1", "4"), ("1", "5"), ("2", "5"), ("2", "6"), ("3", "6"), ("3", "7"), ("3", "8"), ("4", "9"), ("5", "10"), ("5", "12"), ("6", "9"), ("7", "10"), ("7", "11"), ("8", "19"),
//      ("9", "12"), ("10", "12"), ("10", "13"), ("11", "14"), ("12", "15"), ("12", "16"), ("13", "16"), ("13", "17"), ("14", "18"), ("14", "19"), ("15", "20"), ("16", "21"), ("16", "23"),
//      ("17", "20"), ("18", "21"), ("18", "22"), ("19", "25"), ("20", "23"), ("21", "24"), ("22", "23"), ("22", "25"), ("23", "27"), ("23", "29"), ("23", "30"), ("24", "26"), ("24", "28"),
//      ("25", "28"), ("26", "31"), ("27", "32"), ("27", "34"), ("28", "31"), ("29", "32"), ("29", "33"), ("30", "36"), ("31", "37"), ("32", "35"), ("33", "34"), ("33", "36"), ("34", "37"),
//      ("34", "40"), ("35", "38"), ("35", "41"), ("36", "38"), ("36", "40"), ("37", "42"), ("38", "43"), ("38", "45"), ("39", "42"), ("40", "43"), ("40", "44"), ("41", "47"), ("42", "48"),
//      ("43", "46"), ("44", "45"), ("44", "47"), ("45", "50"), ("46", "48"), ("47", "49"), ("48", "50"), ("49", "50"), ("49", "52"), ("48", "52"), ("47", "53"), ("50", "56"), ("50", "54"),
//      ("51", "54"), ("51", "55"), ("52", "55"), ("52", "56"), ("53", "56"), ("53", "57"), ("53", "58"), ("54", "59"), ("55", "60"), ("55", "62"), ("56", "59"), ("57", "60"), ("57", "61"),
//      ("58", "69"), ("59", "62"), ("60", "62"), ("60", "63"), ("61", "64"), ("62", "65"), ("62", "66"), ("63", "66"), ("63", "67"), ("64", "68"), ("64", "69"), ("65", "70"), ("66", "71"),
//      ("66", "73"), ("67", "70"), ("68", "71"), ("68", "72"), ("69", "75"), ("70", "73"), ("71", "74"), ("72", "73"), ("72", "75"), ("73", "77"), ("73", "79"), ("73", "80"), ("74", "76"),
//      ("74", "78"), ("75", "78"), ("76", "81"), ("77", "82"), ("77", "84"), ("78", "81"), ("79", "82"), ("79", "83"), ("80", "86"), ("81", "87"), ("82", "85"), ("83", "84"), ("83", "86"),
//      ("84", "87"), ("84", "90"), ("85", "88"), ("85", "91"), ("86", "88"), ("86", "90"), ("87", "92"), ("88", "93"), ("88", "95"), ("89", "92"), ("90", "93"), ("90", "94"), ("91", "97"),
//      ("92", "98"), ("93", "96"), ("94", "95"), ("94", "97"), ("95", "100"), ("96", "98"), ("97", "99"), ("99", "100")
//    ),
    "status" -> List[Tuple2[String, String]](
      ("1", "2"), ("2", "3"), ("2", "6"), ("3", "4"),
      ("4", "5"), ("5", "8"), ("6", "7"), ("7", "8"),
      ("8", "9"), ("8", "10")
    )
//    "status" -> List[Tuple2[String, String]](
//      ("1", "4"), ("1", "5"), ("2", "5"), ("2", "6"), ("3", "6"), ("3", "7"), ("3", "8"), ("4", "9"), ("5", "10"), ("5", "12"), ("6", "9"), ("7", "10"), ("7", "11"), ("8", "19"),
//      ("9", "12"), ("10", "12"), ("10", "13"), ("11", "14"), ("12", "15"), ("12", "16"), ("13", "16"), ("13", "17"), ("14", "18"), ("14", "19"), ("15", "20"), ("16", "21"), ("16", "23"),
//      ("17", "20"), ("18", "21"), ("18", "22"), ("19", "25"), ("20", "23"), ("21", "24"), ("22", "23"), ("22", "25"), ("23", "27"), ("23", "29"), ("23", "30"), ("24", "26"), ("24", "28"),
//      ("25", "28"), ("26", "31"), ("27", "32"), ("27", "34"), ("28", "31"), ("29", "32"), ("29", "33"), ("30", "36"), ("31", "37"), ("32", "35"), ("33", "34"), ("33", "36"), ("34", "37"),
//      ("34", "40"), ("35", "38"), ("35", "41"), ("36", "38"), ("36", "40"), ("37", "42"), ("38", "43"), ("38", "45"), ("39", "42"), ("40", "43"), ("40", "44"), ("41", "47"), ("42", "48"),
//      ("43", "46"), ("44", "45"), ("44", "47"), ("45", "50"), ("46", "48"), ("47", "49"), ("48", "50"), ("49", "50")
//    )
//    "status" -> List[Tuple2[String, String]](
//      ("1", "4"), ("1", "5"), ("2", "5"), ("2", "6"), ("3", "6"), ("3", "7"), ("3", "8"), ("4", "9"), ("5", "10"), ("5", "12"), ("6", "9"), ("7", "10"), ("7", "11"), ("8", "19"),
//      ("9", "12"), ("10", "12"), ("10", "13"), ("11", "14"), ("12", "15"), ("12", "16"), ("13", "16"), ("13", "17"), ("14", "18"), ("14", "19"), ("15", "20"), ("16", "21"), ("16", "23"),
//      ("17", "20"), ("18", "21"), ("18", "22"), ("19", "25"), ("20", "23"), ("21", "24"), ("22", "23"), ("22", "25"), ("23", "27"), ("23", "29"), ("23", "30"), ("24", "26"), ("24", "28"),
//      ("25", "28"), ("26", "31"), ("27", "32"), ("27", "34"), ("28", "31"), ("29", "32"), ("29", "33"), ("30", "36"), ("31", "37"), ("32", "35"), ("33", "34"), ("33", "36"), ("34", "37"),
//      ("34", "40"), ("35", "38"), ("35", "41"), ("36", "38"), ("36", "40"), ("37", "42"), ("38", "43"), ("38", "45"), ("39", "42"), ("40", "43"), ("40", "44"), ("41", "47"), ("42", "48"),
//      ("43", "46"), ("44", "45"), ("44", "47"), ("45", "50"), ("46", "48"), ("47", "49"), ("48", "50"), ("49", "50"), ("49", "52"), ("48", "52"), ("47", "53"), ("50", "56"), ("50", "54"),
//      ("51", "54"), ("51", "55"), ("52", "55"), ("52", "56"), ("53", "56"), ("53", "57"), ("53", "58"), ("54", "59"), ("55", "60"), ("55", "62"), ("56", "59"), ("57", "60"), ("57", "61"),
//      ("58", "69"), ("59", "62"), ("60", "62"), ("60", "63"), ("61", "64"), ("62", "65"), ("62", "66"), ("63", "66"), ("63", "67"), ("64", "68"), ("64", "69"), ("65", "70"), ("66", "71"),
//      ("66", "73"), ("67", "70"), ("68", "71"), ("68", "72"), ("69", "75"), ("70", "73"), ("71", "74"), ("72", "73"), ("72", "75"), ("73", "77"), ("73", "79"), ("73", "80"), ("74", "76"),
//      ("74", "78"), ("75", "78"), ("76", "81"), ("77", "82"), ("77", "84"), ("78", "81"), ("79", "82"), ("79", "83"), ("80", "86"), ("81", "87"), ("82", "85"), ("83", "84"), ("83", "86"),
//      ("84", "87"), ("84", "90"), ("85", "88"), ("85", "91"), ("86", "88"), ("86", "90"), ("87", "92"), ("88", "93"), ("88", "95"), ("89", "92"), ("90", "93"), ("90", "94"), ("91", "97"),
//      ("92", "98"), ("93", "96"), ("94", "95"), ("94", "97"), ("95", "100"), ("96", "98"), ("97", "99"), ("99", "100")
//    )
  )

  // 状态图的构造算法♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥♥
  def initCurrencyGraph(columnName: String): CurrencyGraph = {
    // 为属性构造时效图
    val rule: List[Tuple2[String, String]] = StudentGraphsUtil.ruleMap.getOrElse(columnName, List[Tuple2[String, String]]())
    val currencyGraph: CurrencyGraph = new CurrencyGraph()
    currencyGraph.rule = rule
    if (rule.nonEmpty) {
      // 若为状态时效约束
      if (rule.size != 1) {
        for (tuple <- rule) {
          val srcVertex = currencyGraph.graph.getOrElse(tuple._1,
            {
              val vertex: CurrencyGraph.Vertex = new CurrencyGraph.Vertex(tuple._1)
              currencyGraph.graph.put(tuple._1, vertex)
              vertex
            }
          )
          val destVertex = currencyGraph.graph.getOrElse(tuple._2,
            {
              val vertex: CurrencyGraph.Vertex = new CurrencyGraph.Vertex(tuple._2)
              currencyGraph.graph.put(tuple._2, vertex)
              vertex
            }
          )
          // 添加边
          srcVertex.outVertexes += destVertex.columnValue
          srcVertex.outDegree += 1
          destVertex.inVertexes += srcVertex.columnValue
          destVertex.inDegree += 1
        }
      }
      // 若为单调时效约束，直接返回空的时效图
    }
    currencyGraph.toposort
    currencyGraph
  }
}



