package SparkTest.logFact.hdfs
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import sparkTest.logFact.hdfs.HdfsWatcher


@RunWith(classOf[JUnitRunner]) 
class HdfsWatcherTest extends FunSuite with SharedSparkContext{
  test("obtainde file name") {
    val rep = new HdfsWatcher ("src/test/resources/hdfs", sc.hadoopConfiguration)
    assert(Set("test0.txt", "test1.txt", "test2.txt", "test3.txt") == rep.getFiles().map(_._1).toSet)
  }
  
  test("no file is obtained for empty Folder") {
    val rep = new HdfsWatcher ("src/test/resources/hdfs/hdfs3", sc.hadoopConfiguration)
    assert(Nil == rep.getFiles().map(_._1))
  }
}