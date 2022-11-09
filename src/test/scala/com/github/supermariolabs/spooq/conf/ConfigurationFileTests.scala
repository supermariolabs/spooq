package com.github.supermariolabs.spooq.conf

import com.github.supermariolabs.spooq.Application
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigurationFileTests  extends AnyFlatSpec with Matchers {
  it should "should is valid and contain a Step without Check" in {
    val job = Application.parseJobFile("src/test/resources/conf/001.conf")
    job.steps.length should be(1)
    job.steps.head.check.isEmpty should be(true)
  }

  it should "should is valid and contain a Step with Check" in {
    val job = Application.parseJobFile("src/test/resources/conf/002.conf")
    job.steps.length should be(1)
    job.steps.head.check.isDefined should be(true)
  }

  it should "should raise exception because file is empty" in {
    intercept[Exception] {
      Application.parseJobFile("src/test/resources/conf/empty.conf")
    }
  }

}
