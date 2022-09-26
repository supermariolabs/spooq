package com.github.supermariolabs.spooq.logging

import com.github.supermariolabs.spooq.model.Step

import scala.collection.Map

object CommonUtils {

  def dumpStep(step: Step): String = {
    var dump = ""

    val fields = step.getClass.getDeclaredFields.toList.map(_.getName)
    val values = step.productIterator.toSeq
    fields.zip(values).foreach(kv => {
      val key = kv._1
      val value = kv._2
      //dump+=(s"value type: ${value.getClass.getName}\n")
      value match {
        case Some(v) => {
          if (v.isInstanceOf[Map[String,String]]) {
            dump+=s"$key ->\n"
            v.asInstanceOf[Map[String,String]].foreach(opt => {
              dump+=s"\t${opt._1} -> ${if (opt._1.toLowerCase.contains("password")) "**********" else opt._2}\n"
            })
          }
          else dump+=s"$key -> $v\n"
        }
        case None => {}
        case _ => {
          dump+=s"$key -> ${value.toString.trim}\n"
        }
      }
    })

    dump
  }

}
