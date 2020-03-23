package org.apache.spark.storage.pmof

import com.esotericsoftware.kryo.KryoException
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerInstance, SerializerManager}
import org.apache.spark.storage.BlockId

class PmemBlockInputStream[K, C](pmemBlockOutputStream: PmemBlockOutputStream, serializer: Serializer) {
  val blockId: BlockId = pmemBlockOutputStream.getBlockId()
  val serializerManager: SerializerManager = SparkEnv.get.serializerManager
  val serInstance: SerializerInstance = serializer.newInstance()
  val persistentMemoryWriter: PersistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler
  var pmemInputStream: PmemInputStream = new PmemInputStream(persistentMemoryWriter, blockId.name)
  var inObjStream: DeserializationStream = serInstance.deserializeStream(pmemInputStream)

  var total_records: Long = 0
  var indexInBatch: Int = 0
  var closing: Boolean = false

  loadStream()

  def loadStream(): Unit = {
    total_records = pmemBlockOutputStream.getTotalRecords()
    indexInBatch = 0
  }

  def readNextItem(): (K, C) = {
    if (closing == true) {
      close()
      return null
    }
    try{
      val k = inObjStream.readObject().asInstanceOf[K]
      val c = inObjStream.readObject().asInstanceOf[C]
      indexInBatch += 1
      if (indexInBatch >= total_records) {
        closing = true
      }
      (k, c)
    } catch {
      case ex: KryoException => {
      }
        sys.exit(0)
    }
  }

  def close(): Unit = {
    pmemInputStream.close
    inObjStream = null
  }
}
