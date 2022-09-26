package com.github.supermariolabs.spooq.model

case class Report(
                   index: Int,
                   done: Boolean,
                   reason: Option[String]
                 )
