package sparkTest.logFact.hdfs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import scala.util.matching.Regex

class HdfsWatcher (dir: String, hdfsConfig: Configuration) {
  val pa: Path = new Path(dir)
  val fs = pa.getFileSystem(hdfsConfig)
  val regex: Regex = ".".r
  
  /**
   * getFiles in dir
   */
  
  def getFiles(): List[(String, Long)]  = {
        val path: Path = new Path(dir)
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
        val a: Array[Array[FileStatus]] = arrayOfFileStatus.map(fileStatus => getRecursiveListFiles(fileStatus, regex ))
        a.flatMap(_.toList).filter(_.isFile).map { file => (file.getPath.getName, file.getModificationTime)}.toList
    }
  
  def getFilesPaths(): List[Path]  = {
        val path: Path = new Path(dir)
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
        val a: Array[Array[FileStatus]] = arrayOfFileStatus.map(fileStatus => getRecursiveListFiles(fileStatus, regex ))
        a.flatMap(_.toList).filter(_.isFile).map { file => file.getPath}.toList
    }

  
   private def getRecursiveListFiles(fileStatus:FileStatus, regex: Regex): Array[FileStatus] = {
        val path : Path = fileStatus.getPath
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus = fs.listStatus(path)
        val matches = arrayOfFileStatus.filter(fileStatus => regex.findFirstIn(fileStatus.getPath.toString).isDefined)
        matches ++ arrayOfFileStatus.filter(_.isDirectory).flatMap(getRecursiveListFiles(_,regex))
    }
}