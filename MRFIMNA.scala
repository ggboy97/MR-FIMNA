package wc
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{PrintWriter, StringReader}

import au.com.bytecode.opencsv.CSVReader

object FIMNA {
  def gm-gsk(Flist, G){
    def Llist: String = "DAT"

    def Glist: String = "DAT"

    val sum_load = 0

    val count = 1.8

    val loc = 1

    for(item: Flist){
        if(count > squre(2,loc-1)){
            put(List, item)
            count++
        }else{
            loc++
        }
    }

    val avg = sum_load/G

    if(currentNode.value>avg){
        put(Glist, currentNode)
        push(Llist, currentNode)
    }

    for(int i=0;i<G.length;i++){
        val groupSum = 0
        for(int j=Llist.length;j>0;j--){
            if(abs(groupSum+currentNode.value-avg)<abs(g_load-avg)){
                groupSum +=currentNode.value;
                  put(Glist, currentNode)
                  push(Llist, currentNode)
            }
        }
    }
  }

  def main(args:Array[String]){
    def FILE_NAME: String = "DAT"
    val conf = new SparkConf().setAppName("FIMNA").setMaster("local").set("spark.sql.warehouse.dir","D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3.4\\workplaces")
    val sc = new SparkContext(conf)

    val minSupport=0.5

    val minConfidence=0.8

    val numPartitions=8

    val t = System.nanoTime

    val rdd = sc.textFile("databases\\webdocs.dat")

    val rddmap = rdd.map(i => i.split(" "))


//    val result = data.map{ line =>
//      val reader = new CSVReader(new StringReader(line));
//      println("-"*50)
//      println("line:"+line)
//      line.split(",").foreach(println)
//      reader.readNext()
//    }

    rddmap.cache()

    val fpg = new FPGrowth()

    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)


    val model = fpg.run(rddmap)
    println("-"*50)
    println("Model:"+model)
    val freqItems = new PrintWriter("out\\"+FILE_NAME+"FP_freqItems.txt")

    model.freqItemsets.collect().foreach(itemset=>{
      println(itemset.items.mkString("[", ",", "]")+","+itemset.freq)
      freqItems.println(itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })
    freqItems.close()

    println("====="*50)
    val time=(System.nanoTime - t)*0.000000001

    val rules = new PrintWriter("out\\"+FILE_NAME+"FP_rules.txt")
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      rules.println(rule.antecedent.mkString(",")+"-->"+ rule.consequent.mkString(",")+"-->"+ rule.confidence)
    })
    rules.close()

    println(model.generateAssociationRules(minConfidence).collect().length)


  }
}
