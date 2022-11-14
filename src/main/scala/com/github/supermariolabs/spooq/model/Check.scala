package com.github.supermariolabs.spooq.model

case class Check(
    val size: Option[Integer] = None,
    val complete: Option[Seq[String]] = None,
    val unique: Option[Seq[String]] = None,
    val contain: Option[Seq[Tuple2[String, Seq[String]]]] = None
)

//this values must be same of Check attibute's name
object Check {
  val CHECK_SIZE = "size"
  val CHECK_COMPLETE = "complete"
  val CHECK_UNIQUE = "unique"
  val CHECK_CONTAIN = "contain"
}