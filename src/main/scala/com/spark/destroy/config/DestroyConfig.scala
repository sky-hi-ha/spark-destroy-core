package com.spark.destroy.config

/**
 * 파기 파이프라인 전체 설정.
 *
 * @param targets                      파기 대상 테이블 목록
 * @param backupDatabase               백업 테이블이 생성될 DB
 * @param backupTableSuffix            백업 테이블 접미사 (기본: _CUSBK)
 * @param blockSizeMB                  REPARTITION 힌트 기준 블록 크기 (MB)
 * @param broadcastEnabled             BROADCAST 힌트 사용 여부
 * @param shufflePartitions            비파티션 테이블의 shuffle 병렬도
 * @param enableParquetCommitSettings  Parquet v2 커밋 설정 적용 여부
 * @param enableDynamicPartitionOverwrite  partitionOverwriteMode=dynamic 적용 여부
 * @param startDate                    파기 시작 일자 필터
 * @param endDate                      파기 종료 일자 필터
 * @param executeDate                  실행 기준일 (미래 파티션 스킵용)
 */
case class DestroyConfig(
  targets: Seq[TableTarget],
  backupDatabase: String,
  backupTableSuffix: String = "_CUSBK",
  blockSizeMB: Double = 400.0,
  broadcastEnabled: Boolean = false,
  shufflePartitions: Option[Int] = None,
  enableParquetCommitSettings: Boolean = false,
  enableDynamicPartitionOverwrite: Boolean = false,
  startDate: Option[String] = None,
  endDate: Option[String] = None,
  executeDate: Option[String] = None
)
