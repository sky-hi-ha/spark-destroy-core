package com.spark.destroy.config

sealed trait PartitionStrategy {
  def partitionKey: Option[String]
}

object PartitionStrategy {
  /** 월별 묶음: WHERE bas_dt LIKE '202501%' */
  case class Monthly(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /** 일별 단건: WHERE bas_dt = '20250115' */
  case class Daily(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /** 파티션 전체: PARTITION 절만, WHERE 없음 */
  case class FullPartition(key: String) extends PartitionStrategy {
    override def partitionKey: Option[String] = Some(key)
  }

  /** 비파티션 테이블 */
  case object NonPartition extends PartitionStrategy {
    override def partitionKey: Option[String] = None
  }
}
