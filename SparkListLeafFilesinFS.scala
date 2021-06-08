import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.ArrayBuffer
import java.io.{ObjectInputStream, ObjectOutputStream}
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter
import java.io.DataInputStream

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream) {
    value = new Configuration(false)
    value.readFields(in)
  }
}

object SparkListLeafFilesinFS {

    def getHdfs(path: String): FileSystem = {
      val conf = new Configuration()
      FileSystem.get(URI.create(path), conf)
    }

    def getFilesAndDirs(path: String): Array[Path] = {
      val fs = getHdfs(path).listStatus(new Path(path))
      FileUtil.stat2Paths(fs)
    }

    def getFiles(path: String): Array[String] = {
      getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
      .map(_.toString)
    }

    def getDirs(path: String): Array[String] = {
      getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory)
      .map(_.toString)
    }

    def getAllFiles(path: String): ArrayBuffer[String] = {
      val arr = ArrayBuffer[String]()
      val hdfs = getHdfs(path)
      val getPath = getFilesAndDirs(path)
      getPath.foreach(patha => {
        if (hdfs.getFileStatus(patha).isFile())
          arr += patha.toString
        else {
          arr ++= getAllFiles(patha.toString())
        }
      })
      arr
    }
}

val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()

val defaultFs = spark.sparkContext.broadcast("hdfs://av-distcp-1.av-distcp.root.hwx.site:8020")
val path = "ofs://ozone1/vol-0-38488"
val chunkSize = spark.sparkContext.broadcast(1024)
val resultFile = "/tmp/file-md5-ozone"

val allFiles : ArrayBuffer[String] = SparkListLeafFilesinFS.getAllFiles(path)
val leafFilesRdd = spark.sparkContext.parallelize(allFiles)

val statusRdd = leafFilesRdd.map(file => new Path(file)).map(path => {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "ofs://ozone1")
    val fs = path.getFileSystem(conf)
    val is = fs.open(path)
    val byteArray: Array[Byte] = Array.ofDim[Byte](1024)
    var totalBytes: Int = 0
    var bytesCount: Int = 0
    var numChunks: Int = 0
    val digest: MessageDigest = MessageDigest.getInstance("MD5")
    while (bytesCount != -1) {
        bytesCount = is.read(byteArray)
        if (bytesCount != -1) {
            digest.update(byteArray, 0, bytesCount)
            totalBytes += bytesCount
            numChunks += 1
        }
    }
    is.close()
    val digestBytes: Array[Byte] = digest.digest()
    val hash: String = DatatypeConverter.printHexBinary(digestBytes);
    (path.toString, hash, totalBytes, numChunks)
  }).repartition(1).sortBy(_._1).toDF("Path", "MD5 Hash", "Total Bytes", "Number of chunks").write.option("header",true).mode("overwrite").csv(defaultFs.value + resultFile)

