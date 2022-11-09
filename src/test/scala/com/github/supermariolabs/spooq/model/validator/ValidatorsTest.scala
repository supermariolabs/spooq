package com.github.supermariolabs.spooq.model.validator

import org.scalatest.flatspec.AnyFlatSpec
import com.github.supermariolabs.spooq.model.{Check, Step}
import org.scalatest.matchers.should.Matchers

class ValidatorsTest extends AnyFlatSpec with Matchers {
  ///SQL

  it should "should validate sql Step (ok)" in {
    val s = new Step(id = "sql step", kind = "sql", sql = Some("select * from fake"))
    SqlStepValidator.validate(s)
  }

  it should "should validate sql Step (wrong)" in {
    val s = Step(id = "sql step", kind = "sql")
    intercept[StepValidationException] { SqlStepValidator.validate(s) }
  }

  ///INPUT

  it should "should validate input Step (ok)" in {
    val s = Step(id = "input step", kind = "input", format = Some("avro"))
    InputStepValidator.validate(s)
  }

  it should "should validate input Step (wrong) without format" in {
    val s = Step(id = "input step", kind = "input")
    intercept[StepValidationException] { InputStepValidator.validate(s) }
  }

  ///INPUT-STREAM
  it should "should validate input-stream Step (ok)" in {
    val s = new Step(id = "input-stream step", kind = "input-stream", format = Some("rate"))
    InputStreamStepValidator.validate(s)
  }

  it should "should validate input-stream Step (wrong)" in {
    val s = Step(id = "input-stream step", kind = "input-stream")
    intercept[StepValidationException] { InputStreamStepValidator.validate(s) }
  }

  ///VARIABLE
  it should "should validate variable Step (ok)" in {
    val s = new Step(id = "variable step", kind = "variable", sql = Some("select * from fake"))
    VariableStepValidator.validate(s)
  }

  it should "should validate variable Step (wrong)" in {
    val s = Step(id = "variable step", kind = "variable")
    intercept[StepValidationException] { VariableStepValidator.validate(s) }
  }

  it should "should validate variable Step (check is not supported)" in {
    val s = new Step(id = "variable step", kind = "variable", sql = Some("select * from fake"), check = Some(Check(size = Some(10))))
    intercept[StepValidationException] { VariableStepValidator.validate(s) }
  }
  ///SCRIPT
  it should "should validate script Step (ok)" in {
    val s = Step(id = "script step", kind = "script", jsr223Engine = Some("python"), code = Some("example code"))
    ScriptStepValidator.validate(s)
  }

  it should "should validate script Step (wrong) with only jsr223Engine" in {
    val s = Step(id = "script step", kind = "script", jsr223Engine = Some("python"))
    intercept[StepValidationException] { ScriptStepValidator.validate(s) }
  }

  it should "should validate script Step (wrong) with only code" in {
    val s = Step(id = "script step", kind = "script", code = Some("example code"))
    intercept[StepValidationException] { ScriptStepValidator.validate(s) }
  }

  it should "should validate script Step (wrong) without jsr223Engine and code" in {
    val s = Step(id = "script step", kind = "script")
    intercept[StepValidationException] { ScriptStepValidator.validate(s) }
  }

  it should "should validate script Step (check is not supported)" in {
    val s = new Step(id = "script step", kind = "script", jsr223Engine = Some("python"), code = Some("example code"), check = Some(Check(size = Some(10))))
    intercept[StepValidationException] { ScriptStepValidator.validate(s) }
  }

  ///OUTPUT
  it should "should validate output Step (ok)" in {
    val s = Step(id = "output step", kind = "output", source = Some("input step"), format = Some("csv"))
    OutputStepValidator.validate(s)
  }

  it should "should validate output Step (wrong) with only source" in {
    val s = Step(id = "output step", kind = "output", source = Some("input step"))
    intercept[StepValidationException] { OutputStepValidator.validate(s) }
  }

  it should "should validate output Step (wrong) with only format" in {
    val s = Step(id = "output step", kind = "output", format = Some("csv"))
    intercept[StepValidationException] { OutputStepValidator.validate(s) }
  }

  it should "should validate output Step (wrong) without source and format" in {
    val s = Step(id = "output step", kind = "output")
    intercept[StepValidationException] { OutputStepValidator.validate(s) }
  }


  ///OUTPUT-STREAM
  it should "should validate output-stream Step (ok)" in {
    val s = Step(id = "output-stream step", kind = "output-stream", source = Some("input-stream step"), format = Some("console"))
    OutputStreamStepValidator.validate(s)
  }

  it should "should validate output-stream Step (wrong) with only source" in {
    val s = Step(id = "output-stream step", kind = "output-stream", source = Some("input-stream step"))
    intercept[StepValidationException] { OutputStreamStepValidator.validate(s) }
  }

  it should "should validate output-stream Step (wrong) with only format" in {
    val s = Step(id = "output-stream step", kind = "output-stream", format = Some("console"))
    intercept[StepValidationException] { OutputStreamStepValidator.validate(s) }
  }

  it should "should validate output-stream Step (wrong) without source and format" in {
    val s = Step(id = "output-stream step", kind = "output-stream")
    intercept[StepValidationException] { OutputStreamStepValidator.validate(s) }
  }

  ///UDF
  it should "should validate udf Step (ok)" in {
    val s = new Step(id = "udf step", kind = "udf", claz = Some("com.github.supermariolabs.spooq.udf.example.Foo"))
    UdfStepValidator.validate(s)
  }

  it should "should validate udf Step (wrong)" in {
    val s = Step(id = "udf step", kind = "udf")
    intercept[StepValidationException] { UdfStepValidator.validate(s) }
  }

  it should "should validate udf Step (check is not supported)" in {
    val s = new Step(id = "udf step", kind = "udf", claz = Some("com.github.supermariolabs.spooq.udf.example.Foo"), check = Some(Check(size = Some(10))))
    intercept[StepValidationException] { UdfStepValidator.validate(s) }
  }

  ///CUSTOM
  it should "should validate custom Step (ok)" in {
    val s = new Step(id = "custom step", kind = "custom", claz = Some("com.github.supermariolabs.spooq.custom.example.Foo"))
    CustomStepValidator.validate(s)
  }

  it should "should validate custom Step (wrong)" in {
    val s = Step(id = "custom step", kind = "custom")
    intercept[StepValidationException] { CustomStepValidator.validate(s) }
  }

  it should "should validate custom Step (check is not supported)" in {
    val s = new Step(id = "custom step", kind = "custom", claz = Some("com.github.supermariolabs.spooq.udf.example.Foo"), check = Some(Check(size = Some(10))))
    intercept[StepValidationException] { CustomStepValidator.validate(s) }
  }
}
