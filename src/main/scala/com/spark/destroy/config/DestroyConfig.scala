package com.spark.destroy.config

/**
 * 파기 파이프라인의 전체 설정.
 *
 * 하나의 DestroyConfig로 여러 테이블의 파기를 일괄 제어한다.
 * 테이블별 개별 설정은 [[TableTarget]]에, 공통 설정은 이 클래스에 정의한다.
 *
 * 사용 예:
 * {{{
 *   val config = DestroyConfig(
 *     targets = Seq(target1, target2),
 *     backupDatabase = "backup_db",
 *     broadcastEnabled = true,
 *     executeDate = Some("20260303")
 *   )
 *   DestroyPipeline.run(spark, config)
 * }}}
 *
 * @param targets                      파기 대상 테이블 목록 ([[TableTarget]] 참조)
 * @param backupDatabase               백업 테이블이 생성될 데이터베이스명
 * @param backupTableSuffix            백업 테이블 접미사 (기본: "_CUSBK"). 원본 테이블명 뒤에 붙는다
 * @param blockSizeMB                  REPARTITION 힌트의 기준 블록 크기 (MB).
 *                                     repartition 수 = ceil(파티션크기 / blockSizeMB)
 * @param broadcastEnabled             BROADCAST 힌트 사용 여부.
 *                                     true이면 파기 사전 테이블을 브로드캐스트 조인
 * @param shufflePartitions            비파티션(NonPartition) 테이블의 shuffle 병렬도.
 *                                     지정하면 `spark.sql.shuffle.partitions`을 설정
 * @param enableParquetCommitSettings  Parquet v2 커밋 설정 적용 여부.
 *                                     true이면 ParquetOutputCommitter + SQLHadoopMapReduceCommitProtocol 사용
 * @param enableDynamicPartitionOverwrite  `spark.sql.sources.partitionOverwriteMode=dynamic` 적용 여부.
 *                                     true이면 INSERT OVERWRITE 시 해당 파티션만 덮어쓴다
 * @param startDate                    파기 시작 일자 필터 (YYYYMMDD 또는 YYYYMM).
 *                                     이 날짜 이전의 파티션은 건너뛴다
 * @param endDate                      파기 종료 일자 필터 (YYYYMMDD 또는 YYYYMM).
 *                                     이 날짜 이후의 파티션은 건너뛴다
 * @param executeDate                  실행 기준일 (YYYYMMDD).
 *                                     미래 파티션을 스킵하기 위한 상한선으로 사용
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
