package org.wq.scala.ml

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter
import scala.collection.mutable
import scala.collection.mutable.Set


object FIMNATest {

  def main(args:Array[String]){
    def FILE_NAME: String = "TXT"
    val conf = new SparkConf().setAppName("FIMNATest").setMaster("local").set("spark.sql.warehouse.dir","D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3.4\\workplaces")
    val sc = new SparkContext(conf)

    val minSupport=0.5

    val minConfidence=0.8

    val numPartitions=8

    val t = System.nanoTime

    val data = sc.textFile("databases\\sample_fpgrowth.txt")

    val transactions=data.map(x=>x.split(" "))
    transactions.cache()

    val fpg = new FPGrowth()

    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)


    val model = fpg.run(transactions)
    val freqItems = new PrintWriter("out\\"+FILE_NAME+"freqItems.txt")

    model.freqItemsets.collect().foreach(itemset=>{
      freqItems.println(itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })
    freqItems.close()

    println("====="*50)
    val time=(System.nanoTime - t)*0.000000001

    val rules = new PrintWriter("out\\"+FILE_NAME+"rules.txt")
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      rules.println(rule.antecedent.mkString(",")+"-->"+ rule.consequent.mkString(",")+"-->"+ rule.confidence)
    })
    rules.close()

    println(model.generateAssociationRules(minConfidence).collect().length)

  }
}
