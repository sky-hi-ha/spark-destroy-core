package com.spark.destroy.engine

import com.spark.destroy.config.{DestroyConfig, PartitionStrategy}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSettingsManager {
  private val log = LoggerFactory.getLogger(getClass)

  /** 파이프라인 시작 시 기본 설정 적용 */
  def applyBaseSettings(spark: SparkSession, config: DestroyConfig): Unit = {
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    if (config.enableDynamicPartitionOverwrite)
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (config.enableParquetCommitSettings) {
      spark.conf.set("spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter")
      spark.conf.set("spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    }
  }

  /**
   * convertMetastoreOrc 동적 전환.
   *
   * | 전략           | 건수  | 값    |
   * |----------------|-------|-------|
   * | Monthly        | -     | false |
   * | Daily + 0      | 0     | false |
   * | Daily + 1↑     | 1↑    | true  |
   * | FullPartition  | -     | false |
   * | NonPartition   | -     | false |
   */
  def setConvertMetastoreOrc(spark: SparkSession,
                              strategy: PartitionStrategy,
                              currentCount: Long): Unit = {
    val value = strategy match {
      case PartitionStrategy.Daily(_) if currentCount > 0 => true
      case _ => false
    }
    log.info(s"convertMetastoreOrc=$value (strategy=$strategy, count=$currentCount)")
    spark.conf.set("spark.sql.hive.convertMetastoreOrc", value.toString)
  }

  /** 비파티션 테이블용 shuffle partitions 설정 */
  def applyShufflePartitions(spark: SparkSession, config: DestroyConfig): Unit = {
    config.shufflePartitions.foreach { cnt =>
      spark.sql(s"SET spark.sql.shuffle.partitions=$cnt")
    }
  }
}
