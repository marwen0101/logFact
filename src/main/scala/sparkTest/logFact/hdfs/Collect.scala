package sparkTest.logFact.hdfs
import sparkTest.logFact.Spark
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.log4j.Level
object Collect {
  def collect(path: String) {
    
    println("start collect ====>>>>>>>>>>>>>>>>>")
    val log = LogManager.getLogger("myLogger")
    log.setLevel(Level.WARN)
    val hdfs = new HdfsWatcher (path, Spark.sc.hadoopConfiguration) 
    log.info("startttt collect")
    log.warn("start collect")
    log.error("error")
    Spark.sc.makeRDD(1 to 1000000).count()
    Spark.sc.makeRDD(1 to 1000000).count()
    Spark.sc.makeRDD(1 to 1000000).count()
    Spark.sc.makeRDD(1 to 1000000).count()
    
    log.info("startttt collect")
    log.warn("start collect")
    log.error("error")
    Spark.sc.stop()
    
    /*while (true) {
    
      try {
        val fileList = hdfs.getFilesPaths().map{ x => (x, x.getName) }
        .filter { case (_ , name: String) => ! name.endsWith("#collected") && ! name.endsWith("_COPYING_")}
      fileList.foreach { case (p: Path, name: String) =>
        log.info("start collecting "+ name)
        println(name, Spark.sc.textFile(p.toString()).count())
        //hdfs.fs.renameSnapshot(p, p.getName, "collected+"+p.getName) 
        hdfs.fs.rename(p, new Path(p.toString()+"#collected"))
        log.info("end collecting" + name)
        }
        
      } catch {
        case _ => ()
      }
      
      
    }*/
  
  }
}