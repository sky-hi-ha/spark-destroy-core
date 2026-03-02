package com.spark.destroy.config

/**
 * 파기 대상 테이블의 파티션 처리 전략.
 *
 * Hive 테이블의 파티션 구조에 따라 파기 SQL의 WHERE 절과
 * INSERT OVERWRITE의 PARTITION 절 생성 방식이 달라진다.
 * 또한 `spark.sql.hive.convertMetastoreOrc` 엔진 전환 로직에도 영향을 준다.
 *
 * | 전략            | WHERE 절 예시                          | 엔진 전환 기준             |
 * |-----------------|----------------------------------------|---------------------------|
 * | Monthly("key")  | `WHERE key LIKE '202501%'`             | 항상 Hive SerDe (false)   |
 * | Daily("key")    | `WHERE key = '20250115'`               | 건수 기반 (0→false, 1↑→true) |
 * | FullPartition   | 없음 (PARTITION 절만 사용)               | 항상 Hive SerDe (false)   |
 * | NonPartition    | 없음 (PARTITION 절도 없음)               | 항상 Hive SerDe (false)   |
 */
sealed trait PartitionStrategy {
  /** 파티션 컬럼명. NonPartition이면 None. */
  def partitionKey: Option[String]
}

object PartitionStrategy {

  /**
   * 월별 묶음 처리.
   *
   * 파티션 값의 앞 6자리(YYYYMM)를 기준으로 같은 월의 파티션을 한 번에 처리한다.
   * 예: `bas_dt`가 `20250101`~`20250131`인 파티션들을 `WHERE bas_dt LIKE '202501%'`로 묶는다.
   *
   * @param key 파티션 컬럼명 (예: "bas_dt")
   */
  case class Monthly(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /**
   * 일별 단건 처리.
   *
   * 파티션 값 하나(YYYYMMDD)를 정확히 지정하여 처리한다.
   * 건수가 0이면 Hive SerDe, 1건 이상이면 built-in ORC로 엔진을 전환한다.
   *
   * @param key 파티션 컬럼명 (예: "bas_dt")
   */
  case class Daily(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /**
   * 파티션 전체 덮어쓰기.
   *
   * WHERE 조건 없이 테이블 전체를 대상으로 하되, INSERT OVERWRITE 시
   * PARTITION 절은 포함한다. 모든 파티션을 한 번에 처리할 때 사용.
   *
   * @param key 파티션 컬럼명 (예: "bas_dt")
   */
  case class FullPartition(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /**
   * 비파티션 테이블.
   *
   * 파티션이 없는 테이블을 대상으로 한다.
   * INSERT OVERWRITE 시 PARTITION 절 없이 전체 테이블을 덮어쓴다.
   * `shufflePartitions` 설정이 적용된다.
   */
  case object NonPartition extends PartitionStrategy {
    override def partitionKey: Option[String] = None
  }
}
