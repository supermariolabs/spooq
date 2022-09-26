package com.github.supermariolabs.spooq.ansi

import scala.io.AnsiColor

class AnsiCodes(rich: Boolean=true) {
  val RESET = if (rich) AnsiColor.RESET else ""
  val YELLOW = if (rich) AnsiColor.YELLOW else ""
  val RED = if (rich) AnsiColor.RED else ""
  val CYAN = if (rich) AnsiColor.CYAN else ""
  val GREEN = if (rich) AnsiColor.GREEN else ""
  val BOLD = if (rich) AnsiColor.BOLD else ""
}
