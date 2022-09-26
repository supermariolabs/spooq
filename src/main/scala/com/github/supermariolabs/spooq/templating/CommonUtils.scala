package com.github.supermariolabs.spooq.templating

import freemarker.template.{Configuration, DefaultObjectWrapperBuilder, Template}

import java.io.{StringReader, StringWriter}

object CommonUtils {

  def processString(origStr: String, variables: java.util.Map[String, Any]): String = {
    val model = new java.util.HashMap[String,Any]()
    model.put("variables",variables)

    val cfg = new Configuration(Configuration.VERSION_2_3_0)
    val objectWrapper = (new DefaultObjectWrapperBuilder(Configuration.VERSION_2_3_0))
    cfg.setObjectWrapper(objectWrapper.build())

    val t = new Template("template", new StringReader(origStr.replace("#{","${")), cfg);

    val out = new StringWriter()
    t.process(model, out)

    out.toString
  }
}