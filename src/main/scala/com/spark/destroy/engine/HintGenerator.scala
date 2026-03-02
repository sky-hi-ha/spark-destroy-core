package com.spark.destroy.engine

import com.spark.destroy.config.{DestroyConfig, PartitionStrategy}
import com.spark.destroy.util.TableSizeUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object HintGenerator {
  private val log = LoggerFactory.getLogger(getClass)

  def generate(spark: SparkSession, config: DestroyConfig,
               db: String, table: String,
               strategy: PartitionStrategy, partitionValue: String): String = {

    val srcSize: Long = strategy match {
      case PartitionStrategy.Monthly(key) =>
        TableSizeUtil.getAveragePartitionSize(spark, db, table, key, partitionValue)
      case PartitionStrategy.Daily(key) =>
        TableSizeUtil.getPartitionSize(spark, db, table, key, partitionValue)
      case PartitionStrategy.FullPartition(_) | PartitionStrategy.NonPartition =>
        TableSizeUtil.getTableSize(spark, db, table)
    }

    val blockBytes = config.blockSizeMB * 1024 * 1024
    val repartitionSize = {
      val raw = srcSize / blockBytes
      if (raw == 0) 1L else math.ceil(raw).toLong
    }

    log.info(s"hint: srcSize=${srcSize}bytes, repartition=$repartitionSize, broadcast=${config.broadcastEnabled}")

    if (config.broadcastEnabled)
      s"/*+ BROADCAST(BRWK), REPARTITION($repartitionSize) */"
    else
      s"/*+ REPARTITION($repartitionSize) */"
  }
}
