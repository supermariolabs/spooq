package com.github.supermariolabs.spooq.model

case class Job(
              id: String,
              shortDesc: scala.Option[String],
              desc: scala.Option[String],
              steps: List[Step]
              )
