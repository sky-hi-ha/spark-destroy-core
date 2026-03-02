package com.spark.destroy.engine

import com.spark.destroy.callback.DestroyCallback
import com.spark.destroy.config.{DestroyConfig, PartitionStrategy, TableTarget}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * 파기 전 원본 데이터를 백업 테이블에 복사하는 매니저.
 *
 * 파기는 INSERT OVERWRITE로 원본 테이블을 덮어쓰기 때문에, 복구를 위해
 * 파기 대상 데이터를 사전에 백업한다. 백업 테이블은 원본과 동일한 스키마를 가진다.
 *
 * 백업 테이블 생성 흐름:
 * {{{
 *   1. DROP TABLE IF EXISTS backup_db.cus_info_CUSBK
 *   2. CREATE TABLE backup_db.cus_info_CUSBK LIKE mydb.cus_info
 *   3. ALTER TABLE backup_db.cus_info_CUSBK UNSET TBLPROPERTIES IF EXISTS('EXTERNAL')
 * }}}
 *
 * UNSET EXTERNAL은 원본이 EXTERNAL 테이블일 경우 백업 테이블은
 * MANAGED로 만들어 DROP 시 데이터도 함께 삭제되도록 보장한다.
 */
object BackupManager {
  private val log = LoggerFactory.getLogger(getClass)

  /** 백업 테이블명 생성. 원본 테이블명 + 접미사 (예: "cus_info" + "_CUSBK" → "cus_info_CUSBK") */
  def backupTableName(tableName: String, suffix: String): String =
    s"${tableName}${suffix}"

  /**
   * 백업 테이블을 생성한다 (DROP → CREATE LIKE → UNSET EXTERNAL).
   *
   * @return 생성된 백업 테이블명 (DB prefix 없이)
   */
  def createBackupTable(spark: SparkSession, config: DestroyConfig,
                        target: TableTarget, callback: DestroyCallback): String = {
    val bkTable = backupTableName(target.table, config.backupTableSuffix)
    val bkFull = s"${config.backupDatabase}.$bkTable"
    val srcFull = s"${target.database}.${target.table}"

    val ddlStatements = Seq(
      s"DROP TABLE IF EXISTS $bkFull",
      s"CREATE TABLE $bkFull LIKE $srcFull",
      s"ALTER TABLE $bkFull UNSET TBLPROPERTIES IF EXISTS('EXTERNAL')"
    )
    ddlStatements.foreach { sql =>
      callback.onQueryExecute(sql)
      spark.sql(sql)
    }

    callback.onBackupCreated(target.database, target.table, bkFull)
    bkTable
  }

  /**
   * 원본 테이블의 데이터를 백업 테이블로 복사한다.
   *
   * 파티션 전략에 따라 INSERT 방식이 달라진다:
   *  - Monthly: `INSERT OVERWRITE ... WHERE key LIKE 'YYYYMM%'`
   *  - Daily: `INSERT OVERWRITE ... WHERE key = 'YYYYMMDD'`
   *  - FullPartition: `INSERT OVERWRITE ... PARTITION(key)` (전체)
   *  - NonPartition: `INSERT INTO ...` (전체)
   *
   * @param backupTable   백업 테이블명 (DB prefix 없이)
   * @param columns       컬럼 목록 문자열 ("ARWK.col1,ARWK.col2,...")
   * @param partitionValue 파티션 값 (Monthly/Daily에서 필수)
   */
  def copyToBackup(spark: SparkSession, config: DestroyConfig,
                   target: TableTarget, backupTable: String,
                   columns: String, partitionValue: Option[String],
                   callback: DestroyCallback): Unit = {
    val bkFull = s"${config.backupDatabase}.$backupTable"
    val srcFull = s"${target.database}.${target.table}"

    val sql = target.partitionStrategy match {
      case PartitionStrategy.NonPartition =>
        s"""INSERT INTO TABLE $bkFull
           | SELECT ARWK.*
           | FROM $srcFull ARWK""".stripMargin

      case PartitionStrategy.Monthly(key) =>
        val pv = partitionValue.getOrElse(
          throw new IllegalArgumentException("Monthly requires partitionValue"))
        s"""INSERT OVERWRITE TABLE $bkFull PARTITION($key)
           | SELECT $columns
           | FROM $srcFull ARWK
           | WHERE $key LIKE '${pv}%'""".stripMargin

      case PartitionStrategy.Daily(key) =>
        val pv = partitionValue.getOrElse(
          throw new IllegalArgumentException("Daily requires partitionValue"))
        s"""INSERT OVERWRITE TABLE $bkFull PARTITION($key)
           | SELECT $columns
           | FROM $srcFull ARWK
           | WHERE $key = '$pv'""".stripMargin

      case PartitionStrategy.FullPartition(key) =>
        s"""INSERT OVERWRITE TABLE $bkFull PARTITION($key)
           | SELECT $columns
           | FROM $srcFull ARWK""".stripMargin
    }

    callback.onQueryExecute(sql)
    spark.sql(sql)
  }
}
