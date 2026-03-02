package com.spark.destroy.callback

/**
 * 파기 파이프라인 훅.
 * 모든 메서드는 기본 no-op 구현. 호출자가 필요한 것만 오버라이드한다.
 */
trait DestroyCallback {
  def onTableStart(database: String, table: String): Unit = {}
  def onBackupCreated(database: String, table: String, backupTable: String): Unit = {}
  def onPartitionStart(database: String, table: String, partitionValue: String): Unit = {}
  def onPartitionComplete(database: String, table: String, partitionValue: String,
                          beforeCount: Long, afterCount: Long): Unit = {}
  def onTableComplete(database: String, table: String,
                      totalBeforeCount: Long, totalAfterCount: Long): Unit = {}

  /** 에러 발생 시 호출. true 반환하면 다음 테이블로 계속, false면 중단. */
  def onTableError(database: String, table: String, error: Throwable): Boolean = false

  /** SQL 실행 직전 호출 (감사 로깅용). */
  def onQueryExecute(sql: String): Unit = {}
}

object NoOpCallback extends DestroyCallback
