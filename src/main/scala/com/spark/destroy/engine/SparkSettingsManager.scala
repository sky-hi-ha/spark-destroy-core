package com.spark.destroy.engine

import com.spark.destroy.config.{DestroyConfig, PartitionStrategy}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Spark 세션 설정을 파기 파이프라인 단계별로 동적 전환하는 매니저.
 *
 * 핵심 역할은 `spark.sql.hive.convertMetastoreOrc` 설정을 파티션 전략과 건수에 따라
 * 동적으로 전환하여, 파기 쿼리가 적절한 I/O 엔진(Hive SerDe / built-in ORC)으로
 * 실행되도록 보장하는 것이다.
 *
 * @see [[com.spark.destroy.config.PartitionStrategy]] 전략별 엔진 전환 규칙
 */
object SparkSettingsManager {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * 파이프라인 시작 시 공통 Spark 설정을 적용한다.
   *
   *  - `hive.exec.dynamic.partition.mode=nonstrict` — 동적 파티션 허용
   *  - `spark.sql.autoBroadcastJoinThreshold=-1` — 자동 브로드캐스트 비활성화 (힌트로 명시적 제어)
   *  - (선택) `partitionOverwriteMode=dynamic`
   *  - (선택) Parquet v2 커밋 프로토콜
   */
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
   * `spark.sql.hive.convertMetastoreOrc`를 전략과 건수에 따라 동적 전환한다.
   *
   * 이 설정은 ORC 테이블의 I/O 엔진을 결정한다:
   *  - `true`  → Spark built-in ORC reader/writer (고성능, 스테이징 격리 약함)
   *  - `false` → Hive SerDe (성능 낮음, 스테이징 격리 강함)
   *
   * Daily 전략에서 건수가 1 이상일 때만 built-in ORC를 사용하고,
   * 그 외에는 스테이징 격리가 강한 Hive SerDe를 사용한다.
   *
   * | 전략           | 건수  | 값    |
   * |----------------|-------|-------|
   * | Monthly        | -     | false |
   * | Daily + 0      | 0     | false |
   * | Daily + 1↑     | 1↑    | true  |
   * | FullPartition  | -     | false |
   * | NonPartition   | -     | false |
   *
   * @param strategy     현재 처리 중인 파티션 전략
   * @param currentCount 해당 파티션의 현재 건수 (파기 전 COUNT 결과)
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

  /**
   * 비파티션(NonPartition) 테이블용 shuffle partitions 설정.
   *
   * [[DestroyConfig.shufflePartitions]]이 지정된 경우에만 적용된다.
   * 파티션 테이블은 REPARTITION 힌트로 병렬도를 제어하므로 이 설정이 불필요하다.
   */
  def applyShufflePartitions(spark: SparkSession, config: DestroyConfig): Unit = {
    config.shufflePartitions.foreach { cnt =>
      spark.sql(s"SET spark.sql.shuffle.partitions=$cnt")
    }
  }
}
