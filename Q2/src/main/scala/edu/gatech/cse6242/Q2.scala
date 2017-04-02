package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 {
	case class Edge(src: String, target: String, weight: Int)
	def main(args: Array[String]) {
    	val sc = new SparkContext(new SparkConf().setAppName("Q2"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

    	// read the file
    	val file = sc.textFile("hdfs://localhost:8020" + args(0))
		/* TODO: Needs to be implemented */

		val df = file.map(_.split("\t",-1)).map(p => Edge(p(0), p(1), p(2).trim.toInt)).toDF().filter("weight>1")
		
		var incoming = df.groupBy("target").sum("weight").as("in").toDF()
		var incoming2 = incoming.withColumnRenamed("sum(weight)", "in").toDF()
		
		var outgoing = df.groupBy("src").sum("weight").as("out")
		var outgoing2 = outgoing.withColumnRenamed("sum(weight)", "out").toDF()
		
		var combine = incoming2.join(outgoing2, $"src" === $"target", "outer")
		var combined = combine.withColumn("gross", combine("out")-combine("in")).toDF()
		//combined.select("src", "gross" )
		
		var ss = combined.select(combined("src"), combined("gross")).toDF()
		
		val tabSeparated = ss.rdd.map(_.mkString("\t"))
		//tabSeparated.saveAsTextFile("./test.tsv")


    	// store output on given HDFS path.
    	// YOU NEED TO CHANGE THIS
    	tabSeparated.saveAsTextFile("hdfs://localhost:8020" + args(1))
  	}
}
