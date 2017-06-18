
package sparkTest.logFact
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sparkTest.logFact.hdfs._



object Main {
  def main (arg: Array[String]) {
    //val log = LogManager.getLogger("myLogger")
    //log.setLevel(Level.WARN)
    //val sc_ = Driver.getOrCreatContext
    
    //val config = new SparkConf().setAppName("demo-app").setMaster("spark://mbp-de-marwen.home:7077")
    //val sc = new SparkContext(config)
    
    //Collect.collect("/Users/hasnaoui/Desktop/soft/hdfs")
    //
    //log.info("start collection")
    Collect.collect("/user/mhasnaoui/repo")
    //log.info("end of collection")
    /*log.warn("Hello demo")

    val data = sc.parallelize(1 to 100000)
    println(data.count())
    
    println("ok")

    log.warn("I am done")
    sc.stop()*/
  }
}