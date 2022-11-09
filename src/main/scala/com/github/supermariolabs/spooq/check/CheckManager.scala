package com.github.supermariolabs.spooq.check

import com.amazon.deequ.VerificationSuite
import com.github.supermariolabs.spooq.model.Step
import org.apache.spark.sql.DataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.slf4j.LoggerFactory

import com.github.supermariolabs.spooq.model.Check._

class CheckManagerException(message: String) extends Exception(message: String)

object CheckManager {
  val logger = LoggerFactory.getLogger(this.getClass)

  def parse(command: String, check: com.github.supermariolabs.spooq.model.Check) : Option[Check] = {
    command match {
      case CHECK_SIZE => if(check.size.isDefined) {
        Some(Check(CheckLevel.Error, "integrity size checks").hasSize(_ == check.size.get))
      } else {
        None
      }
      case CHECK_COMPLETE => if(check.complete.isDefined) {
        Some(Check(CheckLevel.Error, "integrity complete checks").isComplete(check.complete.get))
      } else {
        None
      }
      case CHECK_UNIQUE => if(check.unique.isDefined) {
        Some(Check(CheckLevel.Error, "integrity unique checks").isUnique(check.unique.get))
      } else {
        None
      }
      case CHECK_CONTAIN => if(check.contain.isDefined) {
        Some(Check(CheckLevel.Error, "integrity contain checks").isContainedIn(check.contain.get._1, check.contain.get._2.toArray))
      } else {
        None
      }
    }
  }

  def execute(step: Step, df: DataFrame) = {

    val _verificationRunBuilder = VerificationSuite()
      .onData(df)

    val fields = classOf[com.github.supermariolabs.spooq.model.Check].getDeclaredFields.map(_.getName).toSeq

    val verificationResult = fields.foldLeft(_verificationRunBuilder) {
      (_verificationRunBuilder, field) =>
        parse(field, step.check.get) match {
          case Some(c) => _verificationRunBuilder.addCheck (c)
          case None => _verificationRunBuilder
        }
    }.run()

      if (verificationResult.status != CheckStatus.Success) {
        val msg = s"We found an warning or errors during step id = '${step.id}' check"

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result => logger.error(s"${result.constraint}: ${result.message.get}") }

        //raise exception (it could be configurable)
        throw new CheckManagerException(msg)
      }
    }
}
