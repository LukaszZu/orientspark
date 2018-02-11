import com.arangodb.spark.ArangoSpark
import org.apache.avro.ipc.specific.Person
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

import scala.util.Random

case class DataToSave(id: Long, s: String)

case class E(src: Long, dst: Long)

object SaveOrientTest {

  var ORIENTDB_CONNECTION_URL = "remote:localhost/demodb"

  var ORIENTDB_USER = "root"

  var ORIENTDB_PASSWORD = "ala"

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("aaa")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "192.168.100.105")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .config("spark.rdd.compress", "true")
      .config("spark.storage.memoryFraction", "1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("arangodb.host", "192.168.100.105")
      .config("arangodb.port", "8529")
      .config("arangodb.user", "root")
      .config("arangodb.password", "ala")
      .getOrCreate()

    import ss.implicits._

    val tuples = Seq(
      ("1", "2"),
      ("1", "3"),
      ("2", "4"),
      ("4", "9"),
      ("5", "6"),
      ("6", "7")
    )

    val r = new Random(23)
//    val edges = ss.range(200000)
//      .map(_ => E(r.nextInt(10000), r.nextInt(10000)))
//      .rdd.repartition(100)
//      .map(v => Edge(v.src, v.dst, 1))

    //    val edges = ss.sparkContext.parallelize(tuples.map(f => Edge(f._1, f._2, 1)))


        val dfEdges = ss
          .range(1000000)
          .map(_ => (r.nextInt(100000).toString,r.nextInt(100000).toString))
          .toDF("src", "dst")
    //    val d = ss.range(400).map(c => Edge())
    //    d.show()

//    val graf = Graph.fromEdges(edges, 1)
    //
//    val v1 = graf.connectedComponents(5).vertices.map(_.swap)
//    val v2 = graf.connectedComponents(5).vertices.map(_.swap)
    //
    //    v1.foreach(println)
    //    v2.foreach(println)
    //
//    val j = v1.join(v2)
    //
    //    j.foreach(p => println(p))
//    j.foreach(println)
//    val o = j
//      .toDF()
//      .select($"_2._1" as "k", $"_2._2" as "v")
//      .filter($"k" !== $"v")
//      .groupBy($"k")
//      .agg(collect_list($"v") as "v")
    //
//    o.show(100)

    //     GraphFrame.fromGraphX(graf)
        val gf = GraphFrame.fromEdges(dfEdges)
        ss.sparkContext.setCheckpointDir("/tmp")
        val vv = gf.connectedComponents.run()
        val vv1 = vv.select($"id" as "id1", $"component" as "c1").cache()
        val vv2 = vv.select($"id" as "id2", $"component" as "c2").cache()

        vv1.groupBy($"c1").count().show(100)
//        vv2.count()

//        vv1.join(vv2, $"c1" === $"c2")
//          .select($"id1" as "k", $"id2" as "v")
//          .filter($"k" !== $"v")
//          .groupBy($"k")
//          .agg(collect_list($"v") as "v")
//          .show(100)

    //collectNeighbors(EdgeDirection.In)
    //.foreach(p => println(s"${p._1} -> ${p._2.mkString(":")}"))
    //    ,StorageLevel.MEMORY_ONLY,StorageLevel.MEMORY_ONLY)

    //    ArangoSpark.save(d,"testcolection")
    //    d.write.format("org.apache.spark.sql.cassandra")
    //      .options(Map("table" -> "table2", "keyspace" -> "keyspace1","spark.cassandra.output.batch.grouping.key" -> "replica_set"))
    //      .save()
    //    println("ddddd")
    //    val cc = ss.read.format("org.apache.spark.sql.cassandra")
    //      .options(Map("table" -> "table2", "keyspace" -> "keyspace1")).load().count()
    //
    //    println(cc)
    //        d.write.format("org.apache.spark.orientdb.documents")
    //          .option("dburl", ORIENTDB_CONNECTION_URL)
    //          .option("user", ORIENTDB_USER)
    //          .option("password", ORIENTDB_PASSWORD)
    //          .option("class", "Person1")
    //          .mode(SaveMode.Overwrite)
    //          .save()

  }
}
