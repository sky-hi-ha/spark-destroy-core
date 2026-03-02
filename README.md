# spark-destroy-core

Hive 테이블의 고객 데이터 파기(삭제)를 수행하는 Spark SQL 기반 경량 라이브러리.

SparkSession만 있으면 어디서든 사용할 수 있으며, 운영 환경에서 검증된 파기 패턴(백업 → 엔진 전환 → LEFT JOIN 파기 → 건수 검증)을 캡슐화한다.

## 빌드

```bash
./gradlew build
```

| 항목 | 버전 |
|------|------|
| Scala | 2.12.15 |
| Spark | 3.3.0 (compileOnly) |
| 빌드 | Gradle |

Spark는 `compileOnly`로 선언되어 있어, 클러스터 환경의 Spark에 의존한다.

## 사용법

```scala
import com.spark.destroy._
import com.spark.destroy.config._

val config = DestroyConfig(
  targets = Seq(
    TableTarget("mydb", "cus_info",
      PartitionStrategy.Monthly("bas_dt"),
      JoinCondition.Simple("cus_no", "cus_no"),
      "mydb", "mpn_cus_no")
  ),
  backupDatabase = "backup_db",
  broadcastEnabled = true,
  executeDate = Some("20260303")
)

DestroyPipeline.run(spark, config)
```

## 아키텍처

```
DestroyPipeline.run()                       ← 진입점
  │
  ├─ SparkSettingsManager.applyBaseSettings()  Spark 공통 설정
  │
  └─ for (target <- targets)
       │
       DestroyExecutor.execute(target)       ← 테이블별 오케스트레이션
         │
         ├─ 컬럼 목록 조회         SELECT * FROM table LIMIT 0
         ├─ BackupManager          DROP → CREATE LIKE → UNSET EXTERNAL → INSERT
         ├─ PartitionIterator      SHOW PARTITIONS → 날짜 필터링
         │
         └─ 파티션별 반복:
              ├─ COUNT 조회
              ├─ SparkSettingsManager.setConvertMetastoreOrc()  엔진 전환
              ├─ BackupManager.copyToBackup()                   백업 INSERT
              ├─ HintGenerator.generate()                       REPARTITION/BROADCAST
              ├─ QueryBuilder.buildDestroyQuery()               SQL 조립
              ├─ spark.sql(파기 쿼리 실행)
              └─ COUNT 검증
```

## 패키지 구조

```
com.spark.destroy/
├── DestroyPipeline.scala              진입점 (API)
│
├── config/                            설정 모델 (순수 데이터 타입)
│   ├── PartitionStrategy.scala        파티션 처리 전략 (4종)
│   ├── JoinCondition.scala            조인 조건 (2종)
│   ├── TableTarget.scala              테이블별 파기 대상 정의
│   └── DestroyConfig.scala            파이프라인 전체 설정
│
├── engine/                            SQL 생성 및 Spark 설정
│   ├── SparkSettingsManager.scala     Spark 설정 동적 전환
│   ├── BackupManager.scala            백업 테이블 DDL + INSERT
│   ├── HintGenerator.scala            REPARTITION/BROADCAST 힌트
│   └── QueryBuilder.scala             파기 SQL 조립
│
├── execution/                         실행 제어
│   ├── DestroyExecutor.scala          테이블별 파이프라인 오케스트레이션
│   └── PartitionIterator.scala        파티션 목록 조회 + 필터링
│
├── callback/
│   └── DestroyCallback.scala          생명주기 콜백 (trait)
│
└── util/
    └── TableSizeUtil.scala            HDFS 테이블/파티션 크기 조회
```

## 핵심 설계

### 파티션 전략 (PartitionStrategy)

테이블의 파티션 구조에 따라 WHERE절 생성, 엔진 전환, 파티션 반복 방식이 결정된다.

| 전략 | WHERE절 | 엔진 전환 | 용도 |
|------|---------|-----------|------|
| `Monthly("bas_dt")` | `LIKE '202501%'` | 항상 Hive SerDe | 월별 묶음 처리 |
| `Daily("bas_dt")` | `= '20250115'` | 건수 기반 전환 | 일별 단건 처리 |
| `FullPartition("bas_dt")` | 없음 | 항상 Hive SerDe | 전체 파티션 일괄 |
| `NonPartition` | 없음 | 항상 Hive SerDe | 비파티션 테이블 |

### 조인 조건 (JoinCondition)

파기는 LEFT OUTER JOIN 후 "파기 사전에 매칭되지 않는 행만 남기는" 패턴을 사용한다.

**Simple** — 단일 컬럼 IS NULL 필터
```sql
SELECT ARWK.*
FROM 백업 ARWK
LEFT OUTER JOIN 파기사전 BRWK ON ARWK.cus_no = BRWK.cus_no
WHERE BRWK.cus_no IS NULL
```

**MultiColumnDate** — 다중 컬럼 + 날짜 비교
```sql
SELECT ARWK.*
FROM 백업 ARWK
LEFT OUTER JOIN 파기사전 BRWK
  ON ARWK.intg_mb_no = BRWK.intg_mb_no
 AND ARWK.tup_id = BRWK.tup_id
WHERE (ARWK.bas_dt > BRWK.bas_dt OR BRWK.intg_mb_no IS NULL)
```

### 엔진 전환 (convertMetastoreOrc)

ORC 테이블의 I/O 엔진을 `spark.sql.hive.convertMetastoreOrc` 설정으로 동적 전환한다.

| 값 | 엔진 | 특성 |
|----|------|------|
| `true` | Spark built-in ORC reader/writer | 고성능, 스테이징 격리 약함 |
| `false` | Hive SerDe | 성능 낮음, 스테이징 격리 강함 |

Daily 전략에서만 건수 기반 전환을 수행한다:
- 건수 = 0 → `false` (Hive SerDe)
- 건수 >= 1 → `true` (built-in ORC)

나머지 전략은 안정성을 위해 항상 `false` (Hive SerDe)를 사용한다.

### 백업 흐름

파기는 INSERT OVERWRITE로 원본을 덮어쓰므로, 복구를 위해 사전 백업한다.

```
1. DROP TABLE IF EXISTS backup_db.cus_info_CUSBK
2. CREATE TABLE backup_db.cus_info_CUSBK LIKE mydb.cus_info
3. ALTER TABLE ... UNSET TBLPROPERTIES IF EXISTS('EXTERNAL')
4. INSERT INTO/OVERWRITE backup_db.cus_info_CUSBK SELECT ...
```

UNSET EXTERNAL은 원본이 EXTERNAL 테이블일 때 백업을 MANAGED로 만들어,
DROP 시 데이터도 함께 삭제되도록 보장한다.

### 힌트 생성

HDFS에서 테이블/파티션의 실제 크기를 조회하여 REPARTITION 수를 계산한다.

```
repartition = ceil(파티션 크기(bytes) / blockSizeMB * 1024 * 1024)
```

- Monthly: 해당 월 파티션들의 평균 크기
- Daily: 단일 파티션 크기
- FullPartition / NonPartition: 테이블 전체 크기

### 콜백

`DestroyCallback` trait을 구현하여 파이프라인 생명주기에 훅을 걸 수 있다.

```scala
val auditCallback = new DestroyCallback {
  override def onQueryExecute(sql: String): Unit =
    println(s"[SQL] $sql")

  override def onPartitionComplete(db: String, table: String, pv: String,
                                   before: Long, after: Long): Unit =
    println(s"$db.$table/$pv: $before → $after (${before - after} deleted)")

  override def onTableError(db: String, table: String, e: Throwable): Boolean = {
    println(s"Error on $db.$table: ${e.getMessage}")
    true  // 에러 건너뛰고 다음 테이블 계속
  }
}

DestroyPipeline.run(spark, config, auditCallback)
```

호출 순서:
```
onTableStart
  → onBackupCreated
  → onPartitionStart        (Monthly/Daily만)
      → onQueryExecute      (각 SQL 실행 전)
  → onPartitionComplete     (Monthly/Daily만)
onTableComplete
```

## 설정 옵션 (DestroyConfig)

| 파라미터 | 기본값 | 설명 |
|---------|--------|------|
| `targets` | (필수) | 파기 대상 테이블 목록 |
| `backupDatabase` | (필수) | 백업 테이블이 생성될 DB |
| `backupTableSuffix` | `"_CUSBK"` | 백업 테이블 접미사 |
| `blockSizeMB` | `400.0` | REPARTITION 힌트 기준 블록 크기 |
| `broadcastEnabled` | `false` | BROADCAST 힌트 사용 여부 |
| `shufflePartitions` | `None` | NonPartition용 shuffle 병렬도 |
| `enableParquetCommitSettings` | `false` | Parquet v2 커밋 설정 |
| `enableDynamicPartitionOverwrite` | `false` | 동적 파티션 덮어쓰기 모드 |
| `startDate` | `None` | 파기 시작 일자 필터 |
| `endDate` | `None` | 파기 종료 일자 필터 |
| `executeDate` | `None` | 실행 기준일 (미래 파티션 스킵) |

## 파이프라인 실행 흐름 (상세)

```
DestroyPipeline.run(spark, config, callback)
│
├─ [1] SparkSettingsManager.applyBaseSettings()
│     hive.exec.dynamic.partition.mode = nonstrict
│     spark.sql.autoBroadcastJoinThreshold = -1
│     (선택) partitionOverwriteMode = dynamic
│     (선택) Parquet v2 커밋 프로토콜
│
└─ [2] 테이블별 반복 (DestroyExecutor.execute)
      │
      ├─ 컬럼 목록 조회: SELECT * FROM db.table LIMIT 0
      ├─ 백업 테이블 생성: DROP → CREATE LIKE → UNSET EXTERNAL
      │
      ├─ [Monthly/Daily 전략]
      │     파티션 목록 조회 (SHOW PARTITIONS)
      │     → 날짜 범위 필터링 (startDate, endDate, executeDate)
      │     → 파티션별 내림차순 반복:
      │         ├─ convertMetastoreOrc 설정 (COUNT 읽기용)
      │         ├─ 파기 전 COUNT
      │         ├─ convertMetastoreOrc 재설정 (건수 기반, 쓰기용)
      │         ├─ 백업 INSERT (원본 → 백업)
      │         ├─ TempView 생성 (MultiColumnDate만)
      │         ├─ 힌트 생성 (HDFS 크기 → REPARTITION 수)
      │         ├─ 파기 쿼리 실행 (INSERT OVERWRITE + LEFT OUTER JOIN)
      │         └─ 파기 후 COUNT (검증)
      │
      ├─ [FullPartition 전략]
      │     전체 COUNT → 백업 INSERT → 파기 쿼리 → COUNT 검증
      │
      └─ [NonPartition 전략]
            전체 COUNT → 백업 INSERT → shuffle partitions 설정
            → 파기 쿼리 → COUNT 검증
```
