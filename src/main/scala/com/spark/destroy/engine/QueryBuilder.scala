package com.spark.destroy.engine

import com.spark.destroy.config._

object QueryBuilder {

  /**
   * 파기 쿼리 생성.
   *
   * JoinCondition과 PartitionStrategy 조합으로
   * 운영 코드의 9가지 SQL 패턴을 모두 커버한다.
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
