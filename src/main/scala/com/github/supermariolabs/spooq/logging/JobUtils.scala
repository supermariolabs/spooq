package com.github.supermariolabs.spooq.logging

import com.github.supermariolabs.spooq.model.Job
import org.slf4j.LoggerFactory

class JobUtils(job: Job) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def dump(job: Job): Unit = {
    logger.debug(s"$job")
  }
}
