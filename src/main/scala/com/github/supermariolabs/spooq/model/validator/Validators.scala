package com.github.supermariolabs.spooq.model.validator

import com.github.supermariolabs.spooq.model.Step

trait Validators {
  def validate(step: Step): Unit
}

class StepValidationException(message: String) extends Exception(message: String)

object SqlStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.sql.isDefined) throw new StepValidationException(s"SqlStepValidator: (id = ${step.id}) 'sql' is mandatory")
  }
}

object InputStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.format.isDefined) throw new StepValidationException(s"InputStepValidator: (id = ${step.id}) 'format' is mandatory")
  }
}

object InputStreamStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.format.isDefined) throw new StepValidationException(s"InputStreamStepValidator: (id = ${step.id}) 'format' is mandatory")
  }
}

object VariableStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.sql.isDefined) throw new StepValidationException(s"VariableStepValidator: (id = ${step.id}) 'sql' is mandatory")
    if(step.check.isDefined) throw new StepValidationException(s"VariableStepValidator: (id = ${step.id}) 'check' is not supported")
  }
}

object ScriptStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.jsr223Engine.isDefined || !step.code.isDefined) throw new StepValidationException(s"ScriptStepValidator: (id = ${step.id}) 'jsr223Engine' and 'code' are mandatory")
    if(step.check.isDefined) throw new StepValidationException(s"ScriptStepValidator: (id = ${step.id}) 'check' is not supported")
  }
}

object OutputStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.source.isDefined || !step.format.isDefined) throw new StepValidationException(s"OutputStepValidator: (id = ${step.id}) 'source' and 'format' are mandatory")
  }
}

object OutputStreamStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.source.isDefined || !step.format.isDefined) throw new StepValidationException(s"OutputStreamStepValidator: (id = ${step.id}) 'source' and 'format' are mandatory")
  }
}

object UdfStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.claz.isDefined) throw new StepValidationException(s"UdfStepValidator: (id = ${step.id}) 'claz' is mandatory")
    if(step.check.isDefined) throw new StepValidationException(s"UdfStepValidator: (id = ${step.id}) 'check' is not supported")
  }
}

object CustomStepValidator extends Validators {
  def validate(step: Step): Unit = {
    if(!step.claz.isDefined) throw new StepValidationException(s"CustomStepValidator: (id = ${step.id}) 'claz' is mandatory")
    if(step.check.isDefined) throw new StepValidationException(s"CustomStepValidator: (id = ${step.id}) 'check' is not supported")
  }
}
