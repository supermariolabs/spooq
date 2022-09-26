package com.github.supermariolabs.spooq.metrics

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.slf4j.LoggerFactory

class LoggerListener extends SparkListener {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    if (stageInfo.taskMetrics.inputMetrics.recordsRead>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tRecords read: ${stageInfo.taskMetrics.inputMetrics.recordsRead}")
    }
    if (stageInfo.taskMetrics.inputMetrics.bytesRead>0) {
      logger.debug(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tBytes read: ${stageInfo.taskMetrics.inputMetrics.bytesRead}")
    }
    if (stageInfo.taskMetrics.outputMetrics.recordsWritten>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tRecords written: ${stageInfo.taskMetrics.outputMetrics.recordsWritten}")
    }
    if (stageInfo.taskMetrics.outputMetrics.bytesWritten>0) {
      logger.debug(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tBytes written: ${stageInfo.taskMetrics.outputMetrics.bytesWritten}")
    }
    if (stageInfo.taskMetrics.shuffleReadMetrics.recordsRead>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tShuffle records read: ${stageInfo.taskMetrics.shuffleReadMetrics.recordsRead}")
    }
    if (stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tShuffle total bytes read: ${stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead}")
    }
    if (stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tShuffle records written: ${stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten}")
    }
    if (stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten>0) {
      logger.info(s"\nStage id: ${stageInfo.stageId}, source: ${stageInfo.name}\n\tShuffle bytes written: ${stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten}")
    }
  }}


