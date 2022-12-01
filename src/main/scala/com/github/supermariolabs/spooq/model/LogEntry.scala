package com.github.supermariolabs.spooq.model

case class LogEntry(
                   ts: Long,
                   logger: String,
                   level: String,
                   message: String
                   )
