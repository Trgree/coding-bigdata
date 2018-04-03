package org.ace

object Test {
  def main(args:Array[String]): Unit ={
    val conf=new SparkSession()
    val sc=new SparkContext(conf)
    val text=sc.textFile("file:///usr/local/spark/README.md")
    val result=text.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_).collect()
    result.foreach(println)
  }
}
