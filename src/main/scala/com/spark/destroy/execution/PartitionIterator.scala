package com.spark.destroy.execution

import com.spark.destroy.config.PartitionStrategy
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PartitionIterator {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * 파티션 값 목록 조회 (내림차순).
   * Monthly: 6자리 prefix로 distinct.
   * Daily: 전체 파티션 값.
   */
  def getPartitionValues(spark: SparkSession, db: String, table: String,
                         strategy: PartitionStrategy): Seq[String] = {
    strategy match {
      case PartitionStrategy.Monthly(_) =>
        spark.sql(s"SHOW PARTITIONS $db.$table")
          .collect()
          .map(_.getString(0).split("=")(1))
          .map(v => v.substring(0, Math.min(6, v.length)))
          .distinct
          .sorted
          .reverse
          .toSeq

      case PartitionStrategy.Daily(_) =>
        spark.sql(s"SHOW PARTITIONS $db.$table")
          .collect()
          .map(_.getString(0).split("=")(1))
          .sorted
          .reverse
          .toSeq

      case _ => Seq.empty
    }
  }

  /** 날짜 범위 및 실행 기준일로 필터링 */
  def filterPartitions(values: Seq[String],
                       startDate: Option[String],
                       endDate: Option[String],
                       executeDate: Option[String]): Seq[String] = {
    values.filter { pv =>
      val inRange = (startDate, endDate) match {
        case (Some(st), Some(ed)) => pv >= st && pv <= ed
        case (Some(st), None)     => pv >= st
        case (None, Some(ed))     => pv <= ed
        case (None, None)         => true
      }
      val notFuture = executeDate match {
        case Some(ed) => pv <= ed.substring(0, pv.length)
        case None     => true
      }
      inRange && notFuture
    }
  }
}
