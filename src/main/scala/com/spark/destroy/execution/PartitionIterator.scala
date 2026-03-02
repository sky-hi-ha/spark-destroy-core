package com.spark.destroy.execution

import com.spark.destroy.config.PartitionStrategy
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Hive 테이블의 파티션 값을 조회하고 필터링하는 유틸리티.
 *
 * `SHOW PARTITIONS` 결과를 파싱하여 파티션 전략에 맞는 값 목록을 반환한다.
 * 날짜 범위(startDate~endDate)와 실행 기준일(executeDate)로 추가 필터링할 수 있다.
 *
 * 파티션 전략별 동작:
 *  - Monthly: 파티션 값(YYYYMMDD)의 앞 6자리(YYYYMM)를 추출하여 distinct → 내림차순
 *  - Daily: 파티션 값(YYYYMMDD) 전체를 내림차순
 *  - FullPartition / NonPartition: 빈 시퀀스 (파티션 반복 불필요)
 */
object PartitionIterator {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * 파티션 값 목록을 조회한다 (내림차순).
   *
   * `SHOW PARTITIONS db.table` 결과에서 파티션 값을 추출한다.
   * 결과 형식: "key=value" → "value" 부분만 파싱.
   *
   * @return 내림차순 정렬된 파티션 값 목록.
   *         Monthly이면 YYYYMM, Daily이면 YYYYMMDD, 그 외 빈 시퀀스.
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

  /**
   * 파티션 값을 날짜 범위와 실행 기준일로 필터링한다.
   *
   * 필터 조건:
   *  1. startDate~endDate 범위 내 (문자열 비교, 양쪽 모두 optional)
   *  2. executeDate 이전 (미래 파티션 스킵).
   *     파티션 값 길이에 맞춰 executeDate를 잘라서 비교한다.
   *     예: pv="202501"(6자리) → executeDate "20260303"을 "202603"으로 잘라서 비교
   *
   * @param values      필터링할 파티션 값 목록
   * @param startDate   시작 일자 (inclusive). None이면 하한 없음
   * @param endDate     종료 일자 (inclusive). None이면 상한 없음
   * @param executeDate 실행 기준일. None이면 미래 필터 없음
   * @return 필터를 통과한 파티션 값 목록 (원래 순서 유지)
   */
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
