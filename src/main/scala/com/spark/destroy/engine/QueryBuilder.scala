package com.spark.destroy.engine

import com.spark.destroy.config._

/**
 * 파기 SQL 쿼리를 조립하는 빌더.
 *
 * [[com.spark.destroy.config.JoinCondition]]과 [[com.spark.destroy.config.PartitionStrategy]]의
 * 조합으로, 운영 코드의 모든 SQL 패턴을 단일 메서드에서 생성한다.
 *
 * 생성되는 SQL 구조:
 * {{{
 *   INSERT OVERWRITE TABLE db.table [PARTITION(key)]
 *   SELECT [hint] columns
 *   FROM 백업테이블 ARWK              -- 또는 TempView
 *   LEFT OUTER JOIN 파기사전 BRWK      -- 또는 사용자 서브쿼리
 *     ON <JoinCondition에 의한 조인 조건>
 *   WHERE <JoinCondition에 의한 필터>
 *     AND <PartitionStrategy에 의한 파티션 필터>
 * }}}
 *
 * 조인 패턴별 WHERE 절:
 *  - Simple: `BRWK.refCol IS NULL` — 파기 대상에 없는 행만 남김
 *  - MultiColumnDate: `(ARWK.date > BRWK.date OR BRWK.col IS NULL)` — 갱신된 행 + 미매칭 행
 */
object QueryBuilder {

  /**
   * 파기 쿼리 SQL 문자열을 생성한다.
   *
   * @param target         파기 대상 테이블 정의
   * @param config         파이프라인 설정 (백업DB, 접미사 등)
   * @param columns        컬럼 목록 문자열 ("ARWK.col1,ARWK.col2,...")
   * @param hint           SQL 힌트 문자열 ([[HintGenerator]]에서 생성)
   * @param partitionValue 파티션 값 (Monthly/Daily에서 WHERE 절에 사용)
   * @param useTempView    true이면 FROM 절에 백업 테이블 대신 TempView "ARWK" 사용
   *                       (MultiColumnDate 패턴에서 사용)
   * @return 실행 가능한 INSERT OVERWRITE SQL 문자열
   */
  def buildDestroyQuery(target: TableTarget, config: DestroyConfig,
                        columns: String, hint: String,
                        partitionValue: Option[String],
                        useTempView: Boolean = false): String = {
    val destFull = s"${target.database}.${target.table}"
    val bkTable = BackupManager.backupTableName(target.table, config.backupTableSuffix)
    val bkFull = s"${config.backupDatabase}.$bkTable"

    // FROM 절: TempView 또는 백업 테이블
    val sourceFrom = if (useTempView) "ARWK" else s"$bkFull ARWK"

    // 파기 사전 테이블 (또는 사용자 서브쿼리)
    val refSource = target.userQuery match {
      case Some(uq) => s"($uq) BRWK"
      case None     => s"${target.referenceDatabase}.${target.referenceTable} BRWK"
    }

    // JOIN ON + WHERE 생성
    val (joinOn, whereFilter) = target.joinCondition match {
      case JoinCondition.Simple(tgtCol, refCol) =>
        (s"ARWK.$tgtCol = BRWK.$refCol",
         s"BRWK.$refCol IS NULL")

      case JoinCondition.MultiColumnDate(tgtCols, refCols, tgtDate, refDate) =>
        val onParts = tgtCols.zip(refCols).map { case (t, r) => s"ARWK.$t = BRWK.$r" }
        (onParts.mkString("\n   AND "),
         s"(ARWK.$tgtDate > BRWK.$refDate OR BRWK.${refCols.head} IS NULL)")
    }

    // INSERT 절
    val insertClause = target.partitionStrategy.partitionKey match {
      case Some(key) => s"INSERT OVERWRITE TABLE $destFull PARTITION($key)"
      case None      => s"INSERT OVERWRITE TABLE $destFull"
    }

    // SELECT 절 (비파티션은 ARWK.*)
    val selectColumns = target.partitionStrategy match {
      case PartitionStrategy.NonPartition => s"$hint ARWK.*"
      case _ => s"$hint $columns"
    }

    // 파티션 필터
    val partitionFilter = (target.partitionStrategy, partitionValue) match {
      case (PartitionStrategy.Monthly(key), Some(pv)) => Some(s"ARWK.$key LIKE '${pv}%'")
      case (PartitionStrategy.Daily(key), Some(pv))   => Some(s"ARWK.$key = '$pv'")
      case _ => None
    }

    // WHERE 조합
    val allWhere = (Seq(whereFilter) ++ partitionFilter.toSeq).mkString("\n   AND ")

    s"""$insertClause
       | SELECT $selectColumns
       | FROM $sourceFrom
       | LEFT OUTER JOIN $refSource
       |   ON $joinOn
       | WHERE $allWhere""".stripMargin
  }
}
