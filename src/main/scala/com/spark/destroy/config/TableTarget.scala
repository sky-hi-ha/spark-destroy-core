package com.spark.destroy.config

/**
 * 단일 테이블에 대한 파기 대상 정의.
 *
 * 하나의 TableTarget은 "어떤 테이블을, 어떤 전략으로, 어떤 조인 방식으로 파기할 것인가"를 정의한다.
 * [[DestroyConfig]]의 `targets`에 여러 개를 지정하면 순차적으로 처리된다.
 *
 * 사용 예:
 * {{{
 *   // 단일 컬럼 조인, 월별 파티션
 *   TableTarget("mydb", "cus_info",
 *     PartitionStrategy.Monthly("bas_dt"),
 *     JoinCondition.Simple("cus_no", "cus_no"),
 *     "mydb", "mpn_cus_no")
 *
 *   // 다중 컬럼 조인, 일별 파티션, 사용자 서브쿼리
 *   TableTarget("mydb", "mbs_info",
 *     PartitionStrategy.Daily("bas_dt"),
 *     JoinCondition.MultiColumnDate(
 *       Seq("intg_mb_no", "tup_id"), Seq("intg_mb_no", "tup_id"),
 *       "bas_dt", "bas_dt"),
 *     "mydb", "mpn_mbs_no",
 *     userQuery = Some("SELECT intg_mb_no, tup_id, bas_dt FROM mydb.custom_ref"))
 * }}}
 *
 * @param database           원본 테이블의 데이터베이스명
 * @param table              원본 테이블명 (파기 대상)
 * @param partitionStrategy  파티션 처리 전략 ([[PartitionStrategy]] 참조)
 * @param joinCondition      파기 사전 테이블과의 조인 조건 ([[JoinCondition]] 참조)
 * @param referenceDatabase  파기 사전(reference) 테이블의 데이터베이스명
 * @param referenceTable     파기 사전 테이블명 (파기 대상 고객 목록이 담긴 테이블)
 * @param userQuery          사용자 정의 서브쿼리. 지정하면 referenceTable 대신 이 쿼리를 서브쿼리로 사용
 */
case class TableTarget(
  database: String,
  table: String,
  partitionStrategy: PartitionStrategy,
  joinCondition: JoinCondition,
  referenceDatabase: String,
  referenceTable: String,
  userQuery: Option[String] = None
)
