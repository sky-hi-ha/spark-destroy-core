package com.spark.destroy.config

/**
 * 파기 대상 테이블과 파기 사전(reference) 테이블 간의 조인 조건.
 *
 * 파기 처리는 LEFT OUTER JOIN 후 "매칭되지 않는 행만 남기는" 패턴을 사용한다.
 * 조인 방식에 따라 ON 절과 WHERE 절이 달라진다.
 *
 * {{{
 *   SELECT ARWK.*
 *   FROM 원본 ARWK
 *   LEFT OUTER JOIN 파기사전 BRWK
 *     ON <JoinCondition이 결정>
 *   WHERE <JoinCondition이 결정>
 * }}}
 */
sealed trait JoinCondition

object JoinCondition {

  /**
   * 단일 컬럼 조인 — IS NULL 필터 패턴.
   *
   * 원본 테이블의 키 컬럼과 파기 사전의 키 컬럼을 1:1 조인한 뒤,
   * 파기 사전에 존재하지 않는(IS NULL) 행만 남긴다.
   *
   * 생성되는 SQL:
   * {{{
   *   ON ARWK.targetColumn = BRWK.referenceColumn
   *   WHERE BRWK.referenceColumn IS NULL
   * }}}
   *
   * @param targetColumn    원본 테이블의 조인 컬럼 (예: "cus_no")
   * @param referenceColumn 파기 사전 테이블의 조인 컬럼 (예: "cus_no")
   */
  case class Simple(
    targetColumn: String,
    referenceColumn: String
  ) extends JoinCondition

  /**
   * 다중 컬럼 조인 + 기준일자 비교 패턴.
   *
   * 복합 키로 조인한 뒤, 원본의 날짜가 사전의 날짜보다 큰 행(갱신된 데이터)이거나
   * 사전에 아예 존재하지 않는 행만 남긴다.
   *
   * 생성되는 SQL:
   * {{{
   *   ON ARWK.targetCols(0) = BRWK.refCols(0)
   *  AND ARWK.targetCols(1) = BRWK.refCols(1)
   *  ...
   *   WHERE (ARWK.targetDateColumn > BRWK.referenceDateColumn
   *      OR  BRWK.refCols(0) IS NULL)
   * }}}
   *
   * 이 패턴은 TempView를 사용하여 백업 테이블을 기준으로 조인한다.
   *
   * @param targetColumns      원본 테이블의 조인 컬럼 목록 (예: Seq("intg_mb_no", "tup_id"))
   * @param referenceColumns   파기 사전 테이블의 조인 컬럼 목록 (예: Seq("intg_mb_no", "tup_id"))
   * @param targetDateColumn   원본 테이블의 기준일자 컬럼 (예: "bas_dt")
   * @param referenceDateColumn 파기 사전 테이블의 기준일자 컬럼 (예: "bas_dt")
   */
  case class MultiColumnDate(
    targetColumns: Seq[String],
    referenceColumns: Seq[String],
    targetDateColumn: String,
    referenceDateColumn: String
  ) extends JoinCondition {
    require(targetColumns.size == referenceColumns.size,
      "targetColumns and referenceColumns must have same size")
    require(targetColumns.nonEmpty, "must have at least one join column")
  }
}
