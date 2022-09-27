package com.github.supermariolabs.spooq.conf

import org.rogach.scallop.ScallopConf

class ApplicationConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val configurationFile = opt[String](required = true, default = Some("spooq.conf"))
  val rich = opt[Boolean](required = true, default = Some(false))
  val format = opt[String](required = false)
  val verbose = opt[Boolean](required = true, default = Some(false))
  val interactive = opt[Boolean](required = true, default = Some(false))
  val thriftServer = opt[Boolean](required = true, default = Some(false))
  val thriftPort = opt[String](required = false)

  verify()
}
