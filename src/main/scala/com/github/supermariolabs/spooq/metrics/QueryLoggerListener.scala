package com.github.supermariolabs.spooq.metrics

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.slf4j.LoggerFactory

class QueryLoggerListener extends QueryExecutionListener {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logger.debug(s"\n[QueryLoggerListener::onSuccess]\nfuncName: $funcName\nqe: ${qe.stringWithStats}\ndurationNs: $durationNs")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logger.debug(s"\n[QueryLoggerListener::onFailure]\nfuncName: $funcName\nqe: ${qe.stringWithStats}\nexception: ${exception.getMessage}")
  }
}
