package com.spark.destroy.engine

import com.spark.destroy.callback.DestroyCallback
import com.spark.destroy.config.{DestroyConfig, PartitionStrategy, TableTarget}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object BackupManager {
  private val log = LoggerFactory.getLogger(getClass)

  def backupTableName(tableName: String, suffix: String): String =
    s"${tableName}${suffix}"

  /** 백업 테이블 생성: DROP → CREATE LIKE → UNSET EXTERNAL */
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

  /** 원본 → 백업 테이블로 데이터 복사 */
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
