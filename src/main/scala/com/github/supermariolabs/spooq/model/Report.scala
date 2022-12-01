package com.github.supermariolabs.spooq.model

case class Report(
                   index: Int,
                   done: ProcessingOutput,
                   reason: Option[String]
                 )
