package com.spark.destroy.execution

import com.spark.destroy.callback.DestroyCallback
import com.spark.destroy.config._
import com.spark.destroy.engine._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * 단일 테이블에 대한 파기 파이프라인을 오케스트레이션하는 실행기.
 *
 * [[com.spark.destroy.DestroyPipeline]]이 테이블 목록을 반복하면서 각 테이블마다
 * 이 클래스의 `execute()`를 호출한다.
 *
 * 파티션 전략에 따른 실행 흐름:
 *
 * '''Monthly / Daily (파티션 반복):'''
 * {{{
 *   1. 컬럼 목록 조회 (SELECT * LIMIT 0)
 *   2. 백업 테이블 생성 (DROP → CREATE LIKE → UNSET EXTERNAL)
 *   3. 파티션 목록 조회 → 날짜 필터링 → 내림차순 반복:
 *      a. COUNT 조회 (엔진: Daily→built-in ORC, Monthly→Hive SerDe)
 *      b. 엔진 전환 (건수 기반: setConvertMetastoreOrc)
 *      c. 백업 INSERT
 *      d. TempView 생성 (MultiColumnDate 패턴만)
 *      e. 힌트 생성 (REPARTITION + BROADCAST)
 *      f. 파기 쿼리 실행 (INSERT OVERWRITE + LEFT OUTER JOIN)
 *      g. 파기 후 건수 검증
 * }}}
 *
 * '''FullPartition / NonPartition (일괄 처리):'''
 * {{{
 *   1. 컬럼 목록 조회 → 백업 테이블 생성 → 전체 COUNT
 *   2. 백업 INSERT → 힌트 생성 → 파기 쿼리 실행 → 건수 검증
 * }}}
 *
 * @param spark    Hive 지원이 활성화된 SparkSession
 * @param config   파이프라인 설정
 * @param callback 생명주기 콜백 (감사 로깅, 진행 추적 등)
 */
class DestroyExecutor(spark: SparkSession, config: DestroyConfig,
                      callback: DestroyCallback) {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * 단일 테이블의 파기 파이프라인을 실행한다.
   *
   * 파티션 전략에 따라 내부적으로 `executePartitioned`, `executeFullPartition`,
   * `executeNonPartition` 중 하나를 호출한다.
   */
  def execute(target: TableTarget): Unit = {
    callback.onTableStart(target.database, target.table)
    val srcFull = s"${target.database}.${target.table}"

    // 1. 컬럼 목록 조회
    val columns = spark.sql(s"SELECT * FROM $srcFull LIMIT 0")
      .schema.fieldNames
      .map(c => s"ARWK.$c")
      .mkString(",")

    // 2. 백업 테이블 생성
    val bkTable = BackupManager.createBackupTable(spark, config, target, callback)

    // 3. MultiColumnDate 조인이면 TempView 패턴 사용
    val useTempView = target.joinCondition match {
      case _: JoinCondition.MultiColumnDate => true
      case _ => false
    }

    target.partitionStrategy match {
      case strategy @ (_: PartitionStrategy.Monthly | _: PartitionStrategy.Daily) =>
        executePartitioned(target, strategy, srcFull, bkTable, columns, useTempView)

      case strategy @ PartitionStrategy.FullPartition(_) =>
        executeFullPartition(target, strategy, srcFull, bkTable, columns, useTempView)

      case PartitionStrategy.NonPartition =>
        executeNonPartition(target, srcFull, bkTable, columns, useTempView)
    }
  }

  private def executePartitioned(target: TableTarget, strategy: PartitionStrategy,
                                  srcFull: String, bkTable: String,
                                  columns: String, useTempView: Boolean): Unit = {
    val bkFull = s"${config.backupDatabase}.$bkTable"
    var totalBefore = 0L
    var totalAfter = 0L

    val allPartitions = PartitionIterator.getPartitionValues(
      spark, target.database, target.table, strategy)
    val filtered = PartitionIterator.filterPartitions(
      allPartitions, config.startDate, config.endDate, config.executeDate)

    for (pv <- filtered) {
      callback.onPartitionStart(target.database, target.table, pv)

      // COUNT용 엔진 설정 (Daily만 true)
      val countOrcMode = strategy match {
        case PartitionStrategy.Daily(_) => "true"
        case _ => "false"
      }
      spark.conf.set("spark.sql.hive.convertMetastoreOrc", countOrcMode)

      // 파기 전 건수
      val beforeCount = countForPartition(srcFull, strategy, pv)
      totalBefore += beforeCount

      // 엔진 전환 (건수 기반)
      SparkSettingsManager.setConvertMetastoreOrc(spark, strategy, beforeCount)

      // 백업 INSERT
      BackupManager.copyToBackup(spark, config, target, bkTable,
        columns, Some(pv), callback)

      // TempView 생성 (Mbs 패턴)
      if (useTempView) {
        createTempView(bkFull, columns, strategy, pv)
      }

      // 힌트 생성
      val hint = HintGenerator.generate(spark, config,
        target.database, target.table, strategy, pv)

      // 파기 쿼리 실행
      val sql = QueryBuilder.buildDestroyQuery(
        target, config, columns, hint, Some(pv), useTempView)
      callback.onQueryExecute(sql)
      spark.sql(sql)

      // 파기 후 건수
      val afterCount = countForPartition(srcFull, strategy, pv)
      totalAfter += afterCount
      callback.onPartitionComplete(target.database, target.table, pv,
        beforeCount, afterCount)
    }

    callback.onTableComplete(target.database, target.table, totalBefore, totalAfter)
  }

  private def executeFullPartition(target: TableTarget, strategy: PartitionStrategy,
                                    srcFull: String, bkTable: String,
                                    columns: String, useTempView: Boolean): Unit = {
    val beforeCount = spark.sql(s"SELECT COUNT(*) FROM $srcFull")
      .collect()(0).getLong(0)

    BackupManager.copyToBackup(spark, config, target, bkTable,
      columns, None, callback)

    spark.conf.set("spark.sql.hive.convertMetastoreOrc", "false")
    val hint = HintGenerator.generate(spark, config,
      target.database, target.table, strategy, "")
    val sql = QueryBuilder.buildDestroyQuery(
      target, config, columns, hint, None, useTempView)
    callback.onQueryExecute(sql)
    spark.sql(sql)

    val afterCount = spark.sql(s"SELECT COUNT(*) FROM $srcFull")
      .collect()(0).getLong(0)
    callback.onTableComplete(target.database, target.table, beforeCount, afterCount)
  }

  private def executeNonPartition(target: TableTarget, srcFull: String,
                                   bkTable: String, columns: String,
                                   useTempView: Boolean): Unit = {
    val beforeCount = spark.sql(s"SELECT COUNT(*) FROM $srcFull")
      .collect()(0).getLong(0)

    BackupManager.copyToBackup(spark, config, target, bkTable,
      columns, None, callback)

    spark.conf.set("spark.sql.hive.convertMetastoreOrc", "false")
    SparkSettingsManager.applyShufflePartitions(spark, config)
    val hint = HintGenerator.generate(spark, config,
      target.database, target.table, PartitionStrategy.NonPartition, "")
    val sql = QueryBuilder.buildDestroyQuery(
      target, config, columns, hint, None, useTempView)
    callback.onQueryExecute(sql)
    spark.sql(sql)

    val afterCount = spark.sql(s"SELECT COUNT(*) FROM $srcFull")
      .collect()(0).getLong(0)
    callback.onTableComplete(target.database, target.table, beforeCount, afterCount)
  }

  private def countForPartition(tableFull: String,
                                 strategy: PartitionStrategy, pv: String): Long = {
    val sql = strategy match {
      case PartitionStrategy.Monthly(key) =>
        s"SELECT COUNT(*) FROM $tableFull WHERE $key LIKE '${pv}%'"
      case PartitionStrategy.Daily(key) =>
        s"SELECT COUNT(*) FROM $tableFull WHERE $key = '$pv'"
      case _ =>
        s"SELECT COUNT(*) FROM $tableFull"
    }
    spark.sql(sql).collect()(0).getLong(0)
  }

  private def createTempView(bkFull: String, columns: String,
                              strategy: PartitionStrategy, pv: String): Unit = {
    val whereClause = strategy match {
      case PartitionStrategy.Monthly(key) => s"WHERE $key LIKE '${pv}%'"
      case PartitionStrategy.Daily(key)   => s"WHERE $key = '$pv'"
      case _                               => ""
    }
    spark.sql(
      s"""SELECT $columns
         |  FROM $bkFull ARWK
         | $whereClause""".stripMargin
    ).createOrReplaceTempView("ARWK")
  }
}
