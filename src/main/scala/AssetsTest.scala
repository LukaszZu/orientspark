import org.antlr.v4.runtime.atn.SemanticContext.OR
import org.apache.avro.ipc.specific.Person
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.orientdb.graphs._
import org.graphframes.examples.Graphs

object AssetsTest {

  var ORIENTDB_CONNECTION_URL = "remote:localhost/demodb"

  var ORIENTDB_USER = "root"

  var ORIENTDB_PASSWORD = "ala"


  import java.util.UUID
  import org.graphframes.GraphFrame
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    val FileAssets = "/home/zulk/tmp/assets/assets"
    val FileAssetsByParents = "/home/zulk/tmp/assets/assetByParentsDF"
    val FileAssetsByChildren = "/home/zulk/tmp/assets/assetByChildrenDF"

    val spark = SparkSession.builder().appName("aaa").master("local[*]").getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp")
    import spark.implicits._

    val assetDF = spark.read.parquet(FileAssets).dropDuplicates("id")
    val assetByParentsDF = spark.read.parquet(FileAssetsByParents)
    val assetByChildrenDF = spark.read.parquet(FileAssetsByChildren)

    //    assetDF.filter($"id" === "ea11675e-0103-4355-87c2-809eb449e95a").show()
    //    assetByChildrenDF.show()
    //    ???
    //    assetByParentsDF.show()

    //    val uuidToLong = udf { s: String => UUID.fromString(s).getMostSignificantBits }
    //
    val edges = assetByChildrenDF.select($"id" as "src", $"assetid" as "dst", $"type")
      .union(assetByParentsDF.select($"id" as "src", $"assetid" as "dst", $"type"))
      .distinct()
      //.where($"src".equalTo("b8d366c4-5707-4edc-b1a2-b2da946a12bc").or($"dst".equalTo("b8d366c4-5707-4edc-b1a2-b2da946a12bc")))
      .cache()
    //
    val graph = GraphFrame(assetDF, edges)

    //    assetDF.write.format("org.apache.spark.orientdb.graphs")
    //      .option("dburl", ORIENTDB_CONNECTION_URL)
    //      .option("user", ORIENTDB_USER)
    //      .option("password", ORIENTDB_PASSWORD)
    //      .option("vertextype", "Asset4")
    //      .mode(SaveMode.Overwrite)
    //      .save()
    //
    //
    //    edges.write
    //      .format("org.apache.spark.orientdb.graphs")
    //      .option("dburl", ORIENTDB_CONNECTION_URL)
    //      .option("user", ORIENTDB_USER)
    //      .option("password", ORIENTDB_PASSWORD)
    //      .option("vertextype", "Asset4")
    //      .option("edgetype", "IsRelatedTo1")
    //      .mode(SaveMode.Overwrite)
    //      .save()
    //    edges.show()
    //    val graph = GraphFrame.fromEdges(edges)
    //
    //    graph.edges.groupBy($"src").count().orderBy($"count" desc).show(false)
    //    graph.pageRank.maxIter(10).run().vertices.orderBy($"pagerank" desc).show(false)
    //    graph.degrees.show(false)


    //        d.write.format("org.apache.spark.orientdb.documents")
    //          .option("dburl", ORIENTDB_CONNECTION_URL)
    //          .option("user", ORIENTDB_USER)
    //          .option("password", ORIENTDB_PASSWORD)
    //          .option("class", "Person1")
    //          .mode(SaveMode.Overwrite)
    //          .save()


    //    val dt = when($"src.id".equalTo("85815351-5927-4d65-b743-0d6c83afc041"), 1).otherwise(0)
    //    val st = when($"dst.id".equalTo("85815351-5927-4d65-b743-0d6c83afc041"), 1).otherwise(0)

    //    graph.aggregateMessages
    //      .sendToSrc(st)
    //      .sendToDst(dt)
    //      .agg(sum($"MSG") as "m")
    //      .orderBy($"m" desc)
    //      .show(100, false)
    //    assetByChildrenDF
    //      .filter($"id".equalTo("ff5954c8-313f-4546-9f2e-163f0711cc3e").or($"assetid".equalTo("ff5954c8-313f-4546-9f2e-163f0711cc3e")))
    //      .show(100,false)

    //    graph
    //      .find("(a)-[]->(b);(b)-[]->(c)")
    //      .filter("a.assettype not in  ('Server','Application') and c.assettype = 'Application' ")
    //        .orderBy($"a.id")
    //      .show(false)

    val g1 = graph
      .find("(a)-[e1]->(b); (b)-[e2]->(c)")
      .filter("a.assettype not in  ('Server','Application') and c.assettype = 'Application' ")
      .select($"a", $"c")

    val g2 = graph
      .find("(a)-[]->(c)")
      .filter("a.assettype not in  ('Application') and c.assettype = 'Application' ")

    g1.union(g2)
      .filter($"c.id" === "0dba7114-aef7-407e-aeb1-2f884d619f54")
      .orderBy($"a.id")
      .show(500, false)

    //    println(c)
    //      .show(100, false)
    //    graph.labelPropagation.maxIter(3).run()
    //      .groupBy($"label")
    //      .agg(collect_list(struct($"id", $"assetname", $"assettype") as "s") as "v")
    //      .select($"label",$"v",size($"v") as "size")
    //      .orderBy($"size" desc)
    //      .show(50,false)

    //      groupBy($"label").count().orderBy($"count" desc).show(100)
    ???
    val cc = graph.stronglyConnectedComponents.maxIter(3).run()

    //    val comp = cc.filter($"id" === "c0ce18e8-b0ad-437e-8fcf-076f45126f46").select($"component")
    //    comp.show()
    //    val comps = comp.collect().map(r => r.getAs[Long]("component"))

    //    val largecommunity = cc.filter($"component" isin (comps: _*))
    //    largecommunity.show(500, false)

    //    largecommunity.filter($"id" === "0dba7114-aef7-407e-aeb1-2f884d619f54").show()
    //    cc.filter($"component" === 21).show(1500,false)

    //    cc.distinct().groupBy($"component").count().orderBy($"count" desc ).show()
    //    ???
    val vv1 = cc.select($"id" as "id1", $"component" as "c1").cache()
    val vv2 = cc.select($"id" as "id2", $"component" as "c2", $"assetname", $"assettype").cache()
    //
    vv1.join(vv2, $"c1" === $"c2")
      .select($"id1" as "k", $"id2" as "v", $"assetname", $"assettype", $"c1")
      .filter($"k" !== $"v")
      .groupBy($"k")
      .agg(collect_list(struct($"v", $"assetname", $"assettype") as "ass") as "v")
      .select($"k", size($"v") as "size", $"v")
      //      .filter($"size" < 920)
      .orderBy($"size" desc)
      //      .filter($"k" isin("b8d366c4-5707-4edc-b1a2-b2da946a12bc", "1b35da06-0b6e-4807-a54e-010ad345e567", "e8d99b03-1c00-4a51-a736-04e406c69ae0"))
      .show(500, false)

  }
}
