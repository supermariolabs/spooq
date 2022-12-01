package com.github.supermariolabs.spooq.model

case class ProcessingOutput(
                           step: Step,
                           logs: List[LogEntry],
                           show: Option[String],
                           exit: Boolean
                           )
