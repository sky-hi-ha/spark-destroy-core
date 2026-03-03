# spark-destroy-core

Hive 테이블의 고객 데이터 파기(삭제)를 수행하는 Spark SQL 기반 경량 라이브러리.

운영 환경에서 검증된 파기 패턴을 캡슐화하여, SparkSession만 있으면 어디서든 사용할 수 있다.

## 목차

1. [빌드 & 빠른 시작](#1-빌드--빠른-시작)
2. [파이프라인 흐름](#2-파이프라인-흐름)
3. [패키지 구조](#3-패키지-구조)
4. [설정 모델 — config/](#4-설정-모델--config)
5. [엔진 — engine/](#5-엔진--engine)
6. [실행 제어 — execution/](#6-실행-제어--execution)
7. [콜백 — callback/](#7-콜백--callback)
8. [유틸리티 — util/](#8-유틸리티--util)

---

## 1. 빌드 & 빠른 시작

```bash
./gradlew build
```

| 항목 | 버전 |
|------|------|
| Scala | 2.12.15 |
| Spark | 3.3.0 (compileOnly) |
| 빌드 | Gradle |

Spark는 `compileOnly`로 선언되어 있어, 실행 시 클러스터 환경의 Spark에 의존한다.

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

---

## 2. 파이프라인 흐름

파기 파이프라인의 전체 실행 순서. 괄호 안은 담당 객체.

```
DestroyPipeline.run(spark, config, callback)
│
├─ Spark 공통 설정 적용 (SparkSettingsManager)
│     hive.exec.dynamic.partition.mode = nonstrict
│     spark.sql.autoBroadcastJoinThreshold = -1
│     (선택) partitionOverwriteMode = dynamic
│     (선택) Parquet v2 커밋 프로토콜
│
└─ 테이블별 순차 반복 (DestroyExecutor)
      │
      ├─ 컬럼 목록 조회    SELECT * FROM db.table LIMIT 0
      ├─ 백업 테이블 생성   DROP → CREATE LIKE → UNSET EXTERNAL (BackupManager)
      │
      ├─ [Monthly / Daily 전략]
      │     파티션 목록 조회 + 날짜 필터링 (PartitionIterator)
      │     → 파티션별 내림차순 반복:
      │         ├─ 파기 전 COUNT
      │         ├─ convertMetastoreOrc 전환 (SparkSettingsManager)
      │         ├─ 백업 INSERT (BackupManager)
      │         ├─ TempView 생성 (MultiColumnDate만)
      │         ├─ 힌트 생성 (HintGenerator)
      │         ├─ 파기 SQL 실행 (QueryBuilder → spark.sql)
      │         └─ 파기 후 COUNT 검증
      │
      ├─ [FullPartition 전략]
      │     COUNT → 백업 INSERT → 파기 SQL → COUNT 검증
      │
      └─ [NonPartition 전략]
            COUNT → 백업 INSERT → shuffle partitions 설정
            → 파기 SQL → COUNT 검증
```

---

## 3. 패키지 구조

```
com.spark.destroy/
├── DestroyPipeline.scala              진입점 (공개 API)
│
├── config/                            설정 모델
│   ├── PartitionStrategy.scala        → 4.1 파티션 전략
│   ├── JoinCondition.scala            → 4.2 조인 조건
│   ├── TableTarget.scala              → 4.3 테이블 파기 대상
│   └── DestroyConfig.scala            → 4.4 전체 설정 옵션
│
├── engine/                            SQL 생성 & Spark 설정
│   ├── SparkSettingsManager.scala     → 5.1 엔진 전환
│   ├── BackupManager.scala            → 5.2 백업
│   ├── HintGenerator.scala            → 5.3 힌트 생성
│   └── QueryBuilder.scala             → 5.4 파기 SQL 조립
│
├── execution/                         실행 제어
│   ├── DestroyExecutor.scala          → 6.1 테이블별 오케스트레이션
│   └── PartitionIterator.scala        → 6.2 파티션 반복
│
├── callback/                          생명주기 훅
│   └── DestroyCallback.scala          → 7. 콜백
│
└── util/                              공통 유틸리티
    └── TableSizeUtil.scala            → 8. HDFS 크기 조회
```

---

## 4. 설정 모델 — config/

파이프라인의 동작을 결정하는 순수 데이터 타입들. 로직 없이 "무엇을, 어떻게 파기할 것인가"만 정의한다.

### 4.1 PartitionStrategy — 파티션 처리 전략

테이블의 파티션 구조에 따라 WHERE절 생성, 엔진 전환, 파티션 반복 방식이 결정된다.

| 전략 | WHERE절 예시 | 엔진 전환 | 용도 |
|------|-------------|-----------|------|
| `Monthly("bas_dt")` | `LIKE '202501%'` | 항상 Hive SerDe | 월별 묶음 처리 |
| `Daily("bas_dt")` | `= '20250115'` | 건수 기반 전환 | 일별 단건 처리 |
| `FullPartition("bas_dt")` | 없음 | 항상 Hive SerDe | 전체 파티션 일괄 |
| `NonPartition` | 없음 | 항상 Hive SerDe | 비파티션 테이블 |

### 4.2 JoinCondition — 조인 조건

파기는 **LEFT OUTER JOIN 후 "파기 사전에 매칭되지 않는 행만 남기는"** 패턴을 사용한다.

**Simple** — 단일 컬럼, IS NULL 필터

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

MultiColumnDate는 TempView를 사용하여 백업 테이블 기준으로 조인한다.

### 4.3 TableTarget — 테이블 파기 대상

하나의 TableTarget은 "어떤 테이블을, 어떤 전략으로, 어떤 조인으로 파기할 것인가"를 정의한다.

| 파라미터 | 설명 |
|---------|------|
| `database` / `table` | 원본 테이블 (파기 대상) |
| `partitionStrategy` | 파티션 처리 전략 (4.1 참조) |
| `joinCondition` | 조인 조건 (4.2 참조) |
| `referenceDatabase` / `referenceTable` | 파기 사전 테이블 (파기할 고객 목록) |
| `userQuery` | (선택) 파기 사전 대신 사용할 서브쿼리 |

### 4.4 DestroyConfig — 전체 설정 옵션

| 파라미터 | 기본값 | 설명 |
|---------|--------|------|
| `targets` | (필수) | 파기 대상 테이블 목록 (TableTarget) |
| `backupDatabase` | (필수) | 백업 테이블이 생성될 DB |
| `backupTableSuffix` | `"_CUSBK"` | 백업 테이블 접미사 |
| `blockSizeMB` | `400.0` | REPARTITION 힌트 기준 블록 크기 (MB) |
| `broadcastEnabled` | `false` | BROADCAST 힌트 사용 여부 |
| `shufflePartitions` | `None` | NonPartition 전용 shuffle 병렬도 |
| `enableParquetCommitSettings` | `false` | Parquet v2 커밋 설정 |
| `enableDynamicPartitionOverwrite` | `false` | 동적 파티션 덮어쓰기 모드 |
| `startDate` | `None` | 파기 시작 일자 필터 (이전 파티션 스킵) |
| `endDate` | `None` | 파기 종료 일자 필터 (이후 파티션 스킵) |
| `executeDate` | `None` | 실행 기준일 (미래 파티션 스킵) |

---

## 5. 엔진 — engine/

Spark 설정 제어, 백업 DDL, SQL 힌트 계산, 파기 쿼리 조립을 담당하는 객체들.

### 5.1 SparkSettingsManager — 엔진 전환

`spark.sql.hive.convertMetastoreOrc`를 파티션 전략과 건수에 따라 동적 전환한다.

| 값 | I/O 엔진 | 특성 |
|----|----------|------|
| `true` | Spark built-in ORC reader/writer | 고성능, 스테이징 격리 약함 (`_temporary/`) |
| `false` | Hive SerDe | 성능 낮음, 스테이징 격리 강함 (`.hive-staging_*`) |

**전환 규칙:**

| 전략 | 건수 | 설정값 | 이유 |
|------|------|--------|------|
| Daily | 0 | `false` | 빈 파티션 → 안전한 SerDe |
| Daily | 1↑ | `true` | 데이터 있음 → 고성능 built-in |
| Monthly | - | `false` | 다수 파티션 묶음 → 격리 우선 |
| FullPartition | - | `false` | 전체 덮어쓰기 → 격리 우선 |
| NonPartition | - | `false` | 전체 덮어쓰기 → 격리 우선 |

추가로 `applyBaseSettings()`에서 파이프라인 시작 시 공통 설정을, `applyShufflePartitions()`에서 NonPartition 전용 병렬도를 적용한다.

### 5.2 BackupManager — 백업

파기는 INSERT OVERWRITE로 원본을 덮어쓰므로, 복구를 위해 사전 백업한다.

**테이블 생성 (createBackupTable):**
```
1. DROP TABLE IF EXISTS backup_db.cus_info_CUSBK
2. CREATE TABLE backup_db.cus_info_CUSBK LIKE mydb.cus_info
3. ALTER TABLE ... UNSET TBLPROPERTIES IF EXISTS('EXTERNAL')
```

UNSET EXTERNAL — 원본이 EXTERNAL 테이블일 때 백업을 MANAGED로 변환하여, DROP 시 데이터도 함께 삭제되도록 보장한다.

**데이터 복사 (copyToBackup):**

| 전략 | INSERT 방식 |
|------|------------|
| Monthly | `INSERT OVERWRITE ... WHERE key LIKE 'YYYYMM%'` |
| Daily | `INSERT OVERWRITE ... WHERE key = 'YYYYMMDD'` |
| FullPartition | `INSERT OVERWRITE ... PARTITION(key)` |
| NonPartition | `INSERT INTO ...` (전체) |

### 5.3 HintGenerator — 힌트 생성

HDFS에서 테이블/파티션의 실제 크기를 조회하여 SQL 힌트를 생성한다.

**REPARTITION 계산:**
```
repartition = ceil(크기(bytes) / blockSizeMB * 1024 * 1024)
```

**크기 조회 방식 (전략별):**
| 전략 | 조회 대상 |
|------|----------|
| Monthly | 해당 월 파티션들의 평균 크기 |
| Daily | 단일 파티션 크기 |
| FullPartition / NonPartition | 테이블 전체 크기 |

`broadcastEnabled=true`이면 BROADCAST(BRWK)가 추가된다.

### 5.4 QueryBuilder — 파기 SQL 조립

JoinCondition과 PartitionStrategy의 조합으로 최종 파기 SQL을 생성한다.

**SQL 구조:**
```sql
INSERT OVERWRITE TABLE db.table [PARTITION(key)]
SELECT [힌트] 컬럼목록
FROM 백업테이블 ARWK
LEFT OUTER JOIN 파기사전 BRWK
  ON [JoinCondition이 결정]
WHERE [JoinCondition 필터] AND [PartitionStrategy 필터]
```

| 구성 요소 | 결정 주체 |
|----------|----------|
| INSERT OVERWRITE + PARTITION | PartitionStrategy |
| SELECT 힌트 | HintGenerator |
| FROM 절 | MultiColumnDate이면 TempView, 아니면 백업 테이블 |
| JOIN ON 절 | JoinCondition |
| WHERE 절 (조인 필터) | JoinCondition (IS NULL / 날짜 비교) |
| WHERE 절 (파티션 필터) | PartitionStrategy (LIKE / = / 없음) |

---

## 6. 실행 제어 — execution/

### 6.1 DestroyExecutor — 테이블별 오케스트레이션

단일 테이블에 대한 파기 파이프라인을 실행한다. 파티션 전략에 따라 3가지 경로로 분기한다.

| 메서드 | 전략 | 동작 |
|--------|------|------|
| `executePartitioned` | Monthly, Daily | 파티션별 반복 (COUNT → 엔진전환 → 백업 → 힌트 → 파기 → 검증) |
| `executeFullPartition` | FullPartition | 일괄 처리 (COUNT → 백업 → 파기 → 검증) |
| `executeNonPartition` | NonPartition | 일괄 처리 + shuffle partitions 설정 |

**Monthly/Daily 전략의 파티션 반복 상세:**

```
for (파티션 ← 내림차순) {
  1. convertMetastoreOrc 설정 (COUNT 읽기용)
  2. 파기 전 COUNT
  3. convertMetastoreOrc 재설정 (건수 기반, 쓰기용)
  4. 백업 INSERT
  5. TempView 생성 (MultiColumnDate만)
  6. 힌트 생성
  7. 파기 쿼리 실행
  8. 파기 후 COUNT (검증)
}
```

### 6.2 PartitionIterator — 파티션 반복

`SHOW PARTITIONS` 결과를 파싱하여 파티션 값 목록을 반환한다.

**getPartitionValues:**

| 전략 | 반환값 | 예시 |
|------|--------|------|
| Monthly | YYYYMM (6자리 distinct, 내림차순) | `["202502", "202501"]` |
| Daily | YYYYMMDD (전체, 내림차순) | `["20250131", "20250130", ...]` |
| 그 외 | 빈 시퀀스 | `[]` |

**filterPartitions:**
- `startDate` ~ `endDate` 범위 필터 (문자열 비교)
- `executeDate` 이후의 미래 파티션 스킵 (파티션 값 길이에 맞춰 자동 절삭)

---

## 7. 콜백 — callback/

`DestroyCallback` trait을 구현하여 파이프라인 생명주기에 훅을 걸 수 있다. 모든 메서드에 기본 no-op 구현이 있어, 필요한 것만 오버라이드하면 된다.

**호출 순서:**

```
onTableStart(db, table)
  └→ onBackupCreated(db, table, backupTable)
  └→ onPartitionStart(db, table, partitionValue)     [Monthly/Daily]
       └→ onQueryExecute(sql)                         [각 SQL 실행 전]
  └→ onPartitionComplete(db, table, pv, before, after) [Monthly/Daily]
onTableComplete(db, table, totalBefore, totalAfter)
```

**에러 처리:**
- `onTableError(db, table, error)` → `true` 반환 시 다음 테이블로 계속, `false`면 파이프라인 중단

**사용 예:**

```scala
val loggingCallback = new DestroyCallback {
  override def onPartitionComplete(db: String, table: String, pv: String,
                                   before: Long, after: Long): Unit =
    println(s"$db.$table/$pv: $before → $after (${before - after} deleted)")

  override def onTableError(db: String, table: String, e: Throwable): Boolean = {
    println(s"Error on $db.$table: ${e.getMessage}")
    true  // 에러 건너뛰고 다음 테이블 진행
  }
}

DestroyPipeline.run(spark, config, loggingCallback)
```

기본값은 `NoOpCallback` (아무 동작 없음).

---

## 8. 유틸리티 — util/

### TableSizeUtil — HDFS 크기 조회

HintGenerator에서 REPARTITION 수를 계산할 때 사용하는 HDFS 크기 조회 유틸리티.

| 메서드 | 용도 | 사용 전략 |
|--------|------|----------|
| `getTableLocation` | Hive Metastore → HDFS URI 조회 | 공통 |
| `getTableSize` | 테이블 전체 크기 (bytes) | FullPartition, NonPartition |
| `getPartitionSize` | 단일 파티션 크기 (bytes) | Daily |
| `getAveragePartitionSize` | 월별 파티션 평균 크기 (bytes) | Monthly |
