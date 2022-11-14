package com.github.supermariolabs.spooq.check

import com.github.supermariolabs.spooq.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.github.supermariolabs.spooq.model.{Check, Step}

class CheckManagerTests extends AnyFlatSpec with Matchers with SparkSuite {

  //****************
  //    SIZE
  //****************
  it should "should pass size check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(size = Some(5))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, "row5")).toDF

    CheckManager.execute(fakestep, df) should be(true)
  }

  it should "should not pass size check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(size = Some(5))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3")).toDF

    intercept[CheckManagerException] {
      CheckManager.execute(fakestep, df)
    }
  }

  //****************
  //    COMPLETE
  //****************
  it should "should pass complete check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(complete = Some(Seq("name")))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, "row5")).toDF("id", "name")

    CheckManager.execute(fakestep, df) should be(true)
  }

  it should "should not pass complete check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(complete = Some(Seq("name")))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, null)).toDF("id", "name")

    intercept[CheckManagerException] {
      CheckManager.execute(fakestep, df)
    }
  }

  //****************
  //    UNIQUE
  //****************
  it should "should pass unique check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(unique = Some(Seq("name")))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, "row5")).toDF("id", "name")

    CheckManager.execute(fakestep, df) should be(true)
  }

  it should "should not pass unique check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(unique = Some(Seq("name")))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row4"), (4, "row4"), (5, "row4")).toDF("id", "name")

    intercept[CheckManagerException] {
      CheckManager.execute(fakestep, df)
    }
  }

  //****************
  //    CONTAINS
  //****************
  it should "should pass contain check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(contain = Some(Seq(("id", Seq("1","2","3","4","5")))))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, "row5")).toDF("id", "name")

    CheckManager.execute(fakestep, df) should be(true)
  }

  it should "should pass not contain check" in {
    val fakestep = Step(id = "test", kind = "test", check = Option(Check(contain = Some(Seq(("id", Seq("1","2","3")))))))

    import sparkSession.implicits._
    val df = Seq((1, "row1"), (2, "row2"), (3, "row3"), (4, "row4"), (5, "row5")).toDF("id", "name")

    intercept[CheckManagerException] {
      CheckManager.execute(fakestep, df)
    }
  }
}

