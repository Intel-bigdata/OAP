package org.apache.spark.sql.execution.datasources.spinach

import java.io.DataInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

case class SpinachMeta(val schema: StructType)

private[spinach] object SpinachMeta {
  def initialize(path: Path, jobConf: Configuration): SpinachMeta = {
    // TODO
    // the meta file is simple as only save the schema in json string.
    val fileIn = new DataInputStream(path.getFileSystem(jobConf).open(path))
    new SpinachMeta(StructType.fromString(fileIn.readUTF()))
  }
}
