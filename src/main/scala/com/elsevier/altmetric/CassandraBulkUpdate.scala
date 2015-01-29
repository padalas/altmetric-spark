package com.elsevier.altmetric

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object CassandraBulkUpdate {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("CassandraUpdate")
    sparkConf.setMaster("mesos://" + scala.io.Source.fromFile("/etc/mesos/zk").getLines.reduceLeft(_ + _))
    sparkConf.set("spark.mesos.executor.home", "/home/ubuntu")
    sparkConf.set("spark.executor.uri", "http://s3-eu-west-1.amazonaws.com/alternative-metrics-infra/spark-1.1.1-bin-hadoop2.4.tgz")
    sparkConf.set("spark.cores.max", "4")
    sparkConf.setJars(Seq("spark-cassandra-connector-assembly-1.1.2-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkConf)

    sc.getConf.set("spark.cassandra.connection.host", "10.10.83.19")

    val sourceFile = sc.textFile("s3n://AKIAJJRVSLEJYVBUFEMQ:dl6Yx2+2hwOClxXpBzfmRONXvCJ10IyjrR1dATFV@alternative-metrics/ams/altmetric/bucketed/mesos/article_metrics").map(_.split(","))

    case class Article(scopus_eid: String, altmetric_citation_id: Int, aggregation_date: java.util.Date, scopus_publication_date: String, scopus_doi: String, scopus_pmid: String, scopus_pii: String, scopus_subject: Set[String], scopus_journal_type: String, scopus_doc_type: String, altmetric_source_count: Map[String, Int], mendeley_source_count: Int, altmetric_bucket_count: Map[String, Int], mendeley_group_count: Int, mendeley_by_county: Map[String, Int], mendeley_by_discipline: Map[String, Int], mendeley_by_status: Map[String, Int])

    val article = sourceFile.map(p => Article(p(0), stripN(p(17)), new java.util.Date(), p(1), p(2), p(3), p(4), Set(p(5)), p(6), p(7), Map(p(8).split(";").flatMap(mapExtract _): _*), stripN(p(9)), Map("mass_media" -> stripN(p(10)), "scholar_activity" -> stripN(p(11)), "scholar_commentary" -> stripN(p(12)), "social_activity" -> stripN(p(13))), stripN(p(18)), Map(p(14).split(";").flatMap(mapExtract _): _*), Map(p(15).split(";").flatMap(mapExtract _): _*), Map(p(16).split(";").flatMap(mapExtract _): _*)))

    article.saveToCassandra("altmetric", "article_metrics_v2", SomeColumns("scopus_eid", "altmetric_citation_id", "aggregation_date", "scopus_publication_date", "scopus_doi", "scopus_pmid", "scopus_pii", "scopus_subject", "scopus_journal_type", "scopus_doc_type", "altmetric_source_count", "mendeley_source_count", "altmetric_bucket_count", "mendeley_group_count", "mendeley_by_county", "mendeley_by_discipline", "mendeley_by_status"))


  }

  def stripN(input: String) = {
    if (input contains "N") {
      0
    } else {
      input.toInt
    }
  }


  def mapExtract(input: String) = {
    if (input contains ":") {
      val split = input split ":"
      Some(split(0), stripN(split(1)))
    } else {
      None
    }
  }

}
