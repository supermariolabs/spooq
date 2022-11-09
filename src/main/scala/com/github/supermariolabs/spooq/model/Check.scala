package com.github.supermariolabs.spooq.model



case class Check(
    val size: Option[Integer],
    val complete: Option[String],
    val unique: Option[String],
    val contain: Option[Tuple2[String, Seq[String]]]
)

//this values must be same of Check attibute's name
object Check {
  val CHECK_SIZE = "size"
  val CHECK_COMPLETE = "complete"
  val CHECK_UNIQUE = "unique"
  val CHECK_CONTAIN = "contain"
}