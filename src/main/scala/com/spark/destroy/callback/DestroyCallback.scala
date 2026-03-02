package com.spark.destroy.callback

/**
 * 파기 파이프라인의 생명주기 콜백.
 *
 * 파기 실행 중 주요 이벤트마다 호출되는 훅을 정의한다.
 * 모든 메서드에 기본 no-op 구현이 제공되므로, 필요한 것만 오버라이드하면 된다.
 *
 * 호출 순서:
 * {{{
 *   onTableStart
 *     └→ onBackupCreated
 *     └→ onPartitionStart       (파티션 테이블만)
 *         └→ onQueryExecute     (백업 INSERT, 파기 SQL 등)
 *     └→ onPartitionComplete    (파티션 테이블만)
 *   onTableComplete
 * }}}
 *
 * 사용 예:
 * {{{
 *   val loggingCallback = new DestroyCallback {
 *     override def onQueryExecute(sql: String): Unit =
 *       println(s"[SQL] $sql")
 *     override def onPartitionComplete(db: String, table: String, pv: String,
 *                                      before: Long, after: Long): Unit =
 *       println(s"$db.$table/$pv: $before → $after (${before - after} deleted)")
 *   }
 *   DestroyPipeline.run(spark, config, loggingCallback)
 * }}}
 */
trait DestroyCallback {

  /** 테이블 파기 시작 시 호출. */
  def onTableStart(database: String, table: String): Unit = {}

  /** 백업 테이블 생성 완료 후 호출. */
  def onBackupCreated(database: String, table: String, backupTable: String): Unit = {}

  /** 개별 파티션 처리 시작 시 호출. (Monthly/Daily 전략만) */
  def onPartitionStart(database: String, table: String, partitionValue: String): Unit = {}

  /**
   * 개별 파티션 처리 완료 후 호출. (Monthly/Daily 전략만)
   *
   * @param beforeCount 파기 전 건수
   * @param afterCount  파기 후 건수
   */
  def onPartitionComplete(database: String, table: String, partitionValue: String,
                          beforeCount: Long, afterCount: Long): Unit = {}

  /**
   * 테이블 전체 파기 완료 후 호출.
   *
   * @param totalBeforeCount 모든 파티션의 파기 전 건수 합계
   * @param totalAfterCount  모든 파티션의 파기 후 건수 합계
   */
  def onTableComplete(database: String, table: String,
                      totalBeforeCount: Long, totalAfterCount: Long): Unit = {}

  /**
   * 테이블 파기 중 에러 발생 시 호출.
   *
   * @return true이면 에러를 건너뛰고 다음 테이블로 진행, false이면 파이프라인 중단
   */
  def onTableError(database: String, table: String, error: Throwable): Boolean = false

  /** SQL 실행 직전 호출. 감사 로깅이나 SQL 기록에 활용. */
  def onQueryExecute(sql: String): Unit = {}
}

/** 아무 동작도 하지 않는 기본 콜백. [[com.spark.destroy.DestroyPipeline.run]]의 기본값. */
object NoOpCallback extends DestroyCallback
