package com.spark.destroy.config

sealed trait JoinCondition

object JoinCondition {
  /**
   * 단일 컬럼 조인 (CustomerDestroy 패턴).
   *
   * 생성 SQL:
   *   ON ARWK.targetColumn = BRWK.referenceColumn
   *   WHERE BRWK.referenceColumn IS NULL
   */
  case class Simple(
    targetColumn: String,
    referenceColumn: String
  ) extends JoinCondition

  /**
   * 다중 컬럼 조인 + 기준일자 비교 (CustomerDestroyMbs 패턴).
   *
   * 생성 SQL:
   *   ON ARWK.targetCols(0) = BRWK.refCols(0)
   *  AND ARWK.targetCols(1) = BRWK.refCols(1)
   *   WHERE (ARWK.targetDateColumn > BRWK.referenceDateColumn
   *      OR  BRWK.refCols(0) IS NULL)
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
