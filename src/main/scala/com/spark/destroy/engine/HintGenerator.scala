package com.spark.destroy.engine

import com.spark.destroy.config.{DestroyConfig, PartitionStrategy}
import com.spark.destroy.util.TableSizeUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * 파기 쿼리의 SQL 힌트(REPARTITION, BROADCAST)를 생성하는 객체.
 *
 * HDFS에서 테이블/파티션의 실제 크기를 조회한 뒤, 설정된 블록 크기(blockSizeMB)로
 * 나누어 최적의 repartition 수를 계산한다.
 *
 * 생성 예시:
 * {{{
 *   // broadcastEnabled=true, 파티션 크기 800MB, blockSizeMB=400
 *   "/*+ BROADCAST(BRWK), REPARTITION(2) */"
 *
 *   // broadcastEnabled=false, 파티션 크기 100MB, blockSizeMB=400
 *   "/*+ REPARTITION(1) */"
 * }}}
 *
 * 크기 조회 방식은 파티션 전략에 따라 다르다:
 *  - Monthly: 해당 월 파티션들의 평균 크기
 *  - Daily: 단일 파티션 크기
 *  - FullPartition / NonPartition: 테이블 전체 크기
 */
object HintGenerator {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * SQL 힌트 문자열을 생성한다.
   *
   * @param db             테이블의 데이터베이스명
   * @param table          테이블명
   * @param strategy       파티션 전략 (크기 조회 방식 결정)
   * @param partitionValue 파티션 값 (Monthly: YYYYMM prefix, Daily: YYYYMMDD)
   * @return SQL SELECT 절에 삽입할 힌트 문자열
   */
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
