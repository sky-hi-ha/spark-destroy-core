package com.spark.destroy.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.slf4j.LoggerFactory

/**
 * HDFS 기반 테이블/파티션 크기 조회 유틸리티.
 *
 * [[com.spark.destroy.engine.HintGenerator]]에서 REPARTITION 힌트의
 * 파티션 수를 계산할 때 사용한다.
 * Hive Metastore에서 테이블 위치를 조회한 뒤, HDFS FileSystem API로 실제 크기를 반환한다.
 */
object TableSizeUtil {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Hive Metastore에서 테이블의 HDFS 위치(URI)를 조회한다.
   *
   * @return 테이블의 HDFS 경로 (예: hdfs://namenode:8020/warehouse/mydb.db/cus_info)
   */
  def getTableLocation(spark: SparkSession, db: String, table: String): java.net.URI = {
    spark.sessionState.catalog
      .getTableMetadata(new TableIdentifier(table, Some(db)))
      .location
  }

  /**
   * 테이블 전체 크기를 바이트 단위로 반환한다.
   *
   * FullPartition, NonPartition 전략에서 사용.
   */
  def getTableSize(spark: SparkSession, db: String, table: String): Long = {
    val loc = getTableLocation(spark, db, table)
    val fs = FileSystem.get(loc, spark.sparkContext.hadoopConfiguration)
    fs.getContentSummary(new Path(loc.toString)).getLength
  }

  /**
   * 특정 파티션 하나의 크기를 바이트 단위로 반환한다.
   *
   * Daily 전략에서 사용. HDFS 경로: `{tableLocation}/{partitionKey}={partitionValue}`
   * 파티션 경로가 존재하지 않으면 0L을 반환한다.
   */
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

  /**
   * Monthly 전략용: prefix에 매칭되는 파티션들의 평균 크기를 바이트 단위로 반환한다.
   *
   * 예를 들어 partitionPrefix가 "202501"이면,
   * `bas_dt LIKE '202501%'`에 해당하는 파티션(20250101~20250131)의 크기를 각각 조회하여
   * 평균을 반환한다. 매칭되는 파티션이 없으면 0L.
   */
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
