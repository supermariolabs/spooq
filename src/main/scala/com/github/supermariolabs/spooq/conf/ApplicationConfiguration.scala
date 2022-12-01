package com.github.supermariolabs.spooq.conf

import org.rogach.scallop.ScallopConf

class ApplicationConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val configurationFile = opt[String](required = false)
  val configurationString = opt[String](required = false)
  val rich = opt[Boolean](required = true, default = Some(false))
  val format = opt[String](required = false)
  val verbose = opt[Boolean](required = true, default = Some(false))
  val interactive = opt[Boolean](required = true, default = Some(false))
  val http = opt[Boolean](required = true, default = Some(false))
  val httpPort = opt[String](required = false)
  val httpCors = opt[Boolean](required = false, default = Some(false))
  val thriftServer = opt[Boolean](required = true, default = Some(false))
  val thriftPort = opt[String](required = false)
  val enableGeo = opt[Boolean](required = false, default = Some(false))

  verify()
}
