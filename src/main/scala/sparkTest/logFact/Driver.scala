package sparkTest.logFact
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object NDriver {
  
  val conf: SparkConf = new SparkConf().setAppName("LogCheck")
  val sc: SparkContext = new SparkContext(conf)
  
  def getOrCreatContext = {
    if (sc == null) {
      new SparkContext(conf)
    } else sc
  }
  
  def stopSparkContext {
    if (!sc.isStopped) sc.stop()
  }
  
}

object Spark {
 
  //Cluster mode : Without setMASTER / conf.set("spark.eventLog.dir" ...set("spark.sql.shuffle.partitions", "12")
  val conf = new SparkConf().setAppName("logFacto")//.setMaster("local[6]") //.setMaster("spark://MacBook-Pro-de-Mohamed.local:7077")
  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.sql.tungsten.enabled", "true")
  conf.set("spark.rdd.compress", "true")
  conf.set("spark.io.compression.codec", "snappy")
  //conf.set("spark.eventLog.dir", "file:///usr/local/spark/temp")
  conf.set("spark.sql.shuffle.partitions", "2010") // Add it in local case 
  //conf.set("spark.executor.memory","4g")
  //conf.set("spark.cores.max", "6")
  val sc = new SparkContext(conf)
  conf.getAll.foreach(println(_))
  sc.setLogLevel("ERROR")
  sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
  //val sqlc = new SQLContext(sc)

}