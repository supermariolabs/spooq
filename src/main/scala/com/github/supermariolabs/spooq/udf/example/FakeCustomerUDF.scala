package com.github.supermariolabs.spooq.udf.example

import com.github.javafaker.Faker
import com.github.supermariolabs.spooq.udf.SimpleUDF
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

class FakeCustomerUDF extends SimpleUDF {
  case class Customer(id: Long, firstName: String, lastName: String, email: String)

  def generate(id: Long): Customer = {
    val faker = new Faker
    val fakeFirstName = faker.name.firstName
    val fakeLastName = faker.name.lastName
    val fakeEmail = (fakeFirstName+"."+fakeLastName+"@"+faker.internet.domainName).toLowerCase

    Customer(id,fakeFirstName,fakeLastName,fakeEmail)
  }

  override val udf: UserDefinedFunction = functions.udf(generate(_:Long))
}
