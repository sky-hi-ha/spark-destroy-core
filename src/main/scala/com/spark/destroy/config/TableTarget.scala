package com.spark.destroy.config

/**
 * 단일 테이블 파기 대상.
 *
 * @param database           원본 테이블 DB
 * @param table              원본 테이블명
 * @param partitionStrategy  파티션 처리 전략
 * @param joinCondition      파기 사전 테이블과의 조인 조건
 * @param referenceDatabase  파기 사전 테이블 DB
 * @param referenceTable     파기 사전 테이블명
 * @param userQuery          사용자 정의 서브쿼리 (referenceTable 대체)
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
