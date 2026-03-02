package com.spark.destroy

import com.spark.destroy.callback.{DestroyCallback, NoOpCallback}
import com.spark.destroy.config.DestroyConfig
import com.spark.destroy.engine.SparkSettingsManager
import com.spark.destroy.execution.DestroyExecutor
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * 파기 파이프라인 진입점.
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
   * 파기 파이프라인 실행.
   *
   * @param spark    Hive 지원이 활성화된 SparkSession
   * @param config   파이프라인 설정
   * @param callback 감사/진행 추적 훅 (기본: NoOpCallback)
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
