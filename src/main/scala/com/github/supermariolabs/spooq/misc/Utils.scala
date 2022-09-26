package com.github.supermariolabs.spooq.misc

object Utils {

  def functionByKind(in: String): String = {
    var kind = in

    var i = kind.indexOf("-")
    while(i>0) {
      kind = kind.substring(0,i)+kind.substring(i+1,kind.size).capitalize
      i = kind.indexOf("-")
    }

    kind.capitalize
  }

}
