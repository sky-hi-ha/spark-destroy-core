package com.spark.destroy.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.slf4j.LoggerFactory

object TableSizeUtil {
  private val log = LoggerFactory.getLogger(getClass)

  def getTableLocation(spark: SparkSession, db: String, table: String): java.net.URI = {
    spark.sessionState.catalog
      .getTableMetadata(new TableIdentifier(table, Some(db)))
      .location
  }

  def getTableSize(spark: SparkSession, db: String, table: String): Long = {
    val loc = getTableLocation(spark, db, table)
    val fs = FileSystem.get(loc, spark.sparkContext.hadoopConfiguration)
    fs.getContentSummary(new Path(loc.toString)).getLength
  }

  def getPartitionSize(spark: SparkSession, db: String, table: String,
                       partitionKey: String, partitionValue: String): Long = {
    val loc = getTableLocation(spark, db, table)
    val path = new Path(s"${loc}/${partitionKey}=${partitionValue}")
    val fs = FileSystem.get(loc, spark.sparkContext.hadoopConfiguration)
    try {
      fs.getContentSummary(path).getLength
    } catch {
      case _: java.io.FileNotFoundException =>
        log.warn("Partition path not found: {}", path)
        0L
    }
  }

  /** 월별(LIKE) 전략용: 매칭되는 파티션들의 평균 크기 */
  def getAveragePartitionSize(spark: SparkSession, db: String, table: String,
                               partitionKey: String, partitionPrefix: String): Long = {
    val partitions = spark.sql(
      s"""SELECT DISTINCT $partitionKey
         |  FROM $db.$table
         | WHERE $partitionKey LIKE '${partitionPrefix}%'
         | SORT BY $partitionKey""".stripMargin
    ).collect()

    if (partitions.isEmpty) return 0L

    val sizes = partitions.map { row =>
      getPartitionSize(spark, db, table, partitionKey, row.get(0).toString)
    }
    sizes.sum / sizes.length
  }
}
