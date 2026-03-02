package com.spark.destroy

import com.spark.destroy.callback.{DestroyCallback, NoOpCallback}
import com.spark.destroy.config.DestroyConfig
import com.spark.destroy.engine.SparkSettingsManager
import com.spark.destroy.execution.DestroyExecutor
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * 파기 파이프라인의 공개 진입점.
 *
 * SparkSession과 [[com.spark.destroy.config.DestroyConfig]]만 있으면
 * 어디서든 고객 파기 파이프라인을 실행할 수 있다.
 *
 * 내부 동작:
 *  1. Spark 세션에 기본 설정 적용 ([[com.spark.destroy.engine.SparkSettingsManager]])
 *  2. 설정에 포함된 테이블 목록을 순차 반복
 *  3. 각 테이블에 대해 [[com.spark.destroy.execution.DestroyExecutor]]가
 *     백업 → 엔진 전환 → 파기 쿼리 → 건수 검증 파이프라인을 실행
 *
 * 에러 처리:
 *  - 특정 테이블에서 예외가 발생하면 [[com.spark.destroy.callback.DestroyCallback.onTableError]]를 호출
 *  - 콜백이 true를 반환하면 에러를 건너뛰고 다음 테이블을 처리
 *  - false를 반환하면 예외를 재발생시켜 파이프라인을 중단
 *
 * 사용 예:
 * {{{
 *   import com.spark.destroy._
 *   import com.spark.destroy.config._
 *
 *   val config = DestroyConfig(
 *     targets = Seq(
 *       TableTarget("mydb", "cus_info",
 *         PartitionStrategy.Monthly("bas_dt"),
 *         JoinCondition.Simple("cus_no", "cus_no"),
 *         "mydb", "mpn_cus_no")
 *     ),
 *     backupDatabase = "backup_db",
 *     broadcastEnabled = true,
 *     executeDate = Some("20260303")
 *   )
 *
 *   DestroyPipeline.run(spark, config)
 * }}}
 */
object DestroyPipeline {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * 파기 파이프라인을 실행한다.
   *
   * @param spark    Hive 지원이 활성화된 SparkSession.
   *                 `enableHiveSupport()`로 생성되어야 한다.
   * @param config   파이프라인 설정. 파기 대상 테이블, 백업 DB, 힌트 설정 등을 포함
   * @param callback 생명주기 콜백. 감사 로깅, 진행 추적, 에러 처리 전략을 정의.
   *                 기본값은 [[com.spark.destroy.callback.NoOpCallback]] (아무 동작 없음)
   */
  def run(spark: SparkSession, config: DestroyConfig,
          callback: DestroyCallback = NoOpCallback): Unit = {
    log.info("DestroyPipeline starting with {} targets", config.targets.size)

    SparkSettingsManager.applyBaseSettings(spark, config)
    val executor = new DestroyExecutor(spark, config, callback)

    for (target <- config.targets) {
      try {
        executor.execute(target)
      } catch {
        case e: Exception =>
          val shouldContinue = callback.onTableError(target.database, target.table, e)
          if (!shouldContinue) {
            log.error(s"Pipeline aborted on ${target.database}.${target.table}")
            throw e
          }
          log.warn(s"Continuing after error on ${target.database}.${target.table}: ${e.getMessage}")
      }
    }

    log.info("DestroyPipeline complete")
  }
}
