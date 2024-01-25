package com.github.supermariolabs.spooq.etl

import com.github.supermariolabs.spooq.etl.RestApiStep.OauthAuthentication
import com.github.supermariolabs.spooq.model.Step
import io.circe.generic.auto._
import io.circe._
import io.circe.parser._
import org.apache.http.auth.AuthenticationException
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpPut, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.asJavaIterableConverter
import scala.util.Try

class RestApiStep extends CustomInputStep {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def run(dfMap: Map[String, DataFrame], varMap: Map[String, Any], args: Map[String, String], customInputStep: Step): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    val reader = spark.read
    customInputStep.options.foreach(optionDefined => {
      optionDefined.filterKeys(!_.startsWith("api_rest_")) foreach (option =>
        reader.option(option._1, option._2))
    })
    /*customInputStep.options.getOrElse(Map.empty).foreach(optionEntry => {
      logger.info(s"Options: ${optionEntry._1} type:\n${optionEntry._2}")
    })*/

    val options = customInputStep.options.getOrElse(Map.empty)
    //options("api_rest_authentication_body")
    customInputStep.format match {
      case Some(format) =>
        if (customInputStep.format.get == "json") {
          val httpClient: CloseableHttpClient = HttpClients.createDefault()
          //authenticate(options, httpClient)
          options.get("api_rest_method") match {
            case Some(method) =>
              customInputStep.path.map(p => {
                method.toUpperCase() match {
                  case "GET" => new HttpGet(p)
                  case "PUT" =>
                    val putRequest = new HttpPut(p)
                    putRequest.setEntity(new StringEntity(options("api_rest_body")))
                    putRequest
                  case "POST" =>
                    val postRequest = new HttpPost(p)
                    postRequest.setEntity(new StringEntity(options("api_rest_body")))
                    postRequest
                  case _ =>
                    httpClient.close()
                    throw new IllegalArgumentException(s"Method not supported: $p. Methods supported: GET, PUT, POST.")
                }
              }) match {
                case Some(request) =>
                  if (options.contains("api_rest_authentication_body")) {
                    val authResponse = authenticate(options, httpClient)
                    val result = EntityUtils.toString(authResponse.getEntity)
                    if (authResponse.getStatusLine.getStatusCode == 200) {
                      val oAuth: Either[Error, OauthAuthentication] = for {
                        json <- parse(result)
                        myJson <- json.as[OauthAuthentication]
                      } yield myJson
                      oAuth match {
                        case Right(oauthAuthentication) =>
                          request.addHeader("Authorization", s"Bearer ${oauthAuthentication.access_token}")
                        case Left(error) =>
                          logger.error(s"Can't parse authentication json: ${error.getMessage}")
                      }
                    } else throw new IllegalArgumentException(s"Error during authentication with code: ${authResponse.getStatusLine.getStatusCode} \n cause: $result")
                  } else logger.info("No authentication specified in the config file. Doing request without authentication...")
                  request.addHeader("Content-Type", "application/json")
                  //request.addHeader("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImtXYmthYTZxczh3c1RuQndpaU5ZT2hIYm5BdyIsImtpZCI6ImtXYmthYTZxczh3c1RuQndpaU5ZT2hIYm5BdyJ9.eyJhdWQiOiJodHRwczovL2lyZW5iZWUub25taWNyb3NvZnQuY29tL0ZlZWRpbmdCZWUiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9iOGM2MTdlZC0wN2EyLTQ2NzktYTdlMi0yODQ1YzYwNzk1NDIvIiwiaWF0IjoxNzA4NDI4OTMzLCJuYmYiOjE3MDg0Mjg5MzMsImV4cCI6MTcwODQzMjgzMywiYWlvIjoiRTJOZ1lEQnBMTDJlK0tZNThiaW9vZDFpaVlLNUFBPT0iLCJhcHBpZCI6ImNhNjA3OTE2LTkxMTYtNDNhZS1iZjk4LWFmNDQ4M2RkMWIwMSIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2I4YzYxN2VkLTA3YTItNDY3OS1hN2UyLTI4NDVjNjA3OTU0Mi8iLCJvaWQiOiJhY2ZmMzFkYi02MDM0LTQ4NmEtYmU1OS03NjQwZTFiZmIyZGQiLCJyaCI6IjAuQVNBQTdSZkd1S0lIZVVhbjRpaEZ4Z2VWUXQwNWliLXpVS0pEdGNWYXhrSmVrelRrQUFBLiIsInN1YiI6ImFjZmYzMWRiLTYwMzQtNDg2YS1iZTU5LTc2NDBlMWJmYjJkZCIsInRpZCI6ImI4YzYxN2VkLTA3YTItNDY3OS1hN2UyLTI4NDVjNjA3OTU0MiIsInV0aSI6IjhRamdDUDdJc2ttdG51cTNvN2t4QUEiLCJ2ZXIiOiIxLjAifQ.grZLUYQv6E50I4y93n-lHAixGkJA7sH5EsyfVFoKv7UFLCIZhULgcuhJ7rTAj-M_kj8y0UnUxwDLP1mgUcvtas4ptASxfc-I7RwMFypCLBD9YruA10-JUytLTZAl0O2NapitdSb6ZxhwfcMsfk5jf4SgKZuna3aEH-Wpj6oBZaEHldTRtuSvCRAXoMvxCkBXfnrbW0kdEhPdp71NqrDF60n59Cl7ACdW4UZAIgzjB8BqfMom3lvwkd4bpQYxaGwT0QYo3aaWUUD9f6Fx8SX1Hot_bX6JsGKMWA9hMaALGbIcp3dy_0qQMBxAMQvY0_xNlCY5YEvXJP0UD_RJllEuqQ")
                  val response = httpClient.execute(request)
                  response.getStatusLine.getStatusCode match {
                    case 200 =>
                      val entity = response.getEntity
                      val data = EntityUtils.toString(entity)
                      httpClient.close()
                      val stringDS = spark.createDataset(data.split("\n"))(Encoders.STRING)
                      /*spark.read
                        .option("multiline", "true")
                        .json(stringDS)*/
                      reader.json(stringDS)
                    case 401 | 403 =>
                      httpClient.close()
                      throw new AuthenticationException(response.getStatusLine.getReasonPhrase)
                    case _ =>
                      httpClient.close()
                      throw new IllegalArgumentException(response.getStatusLine.getReasonPhrase)
                  }
                case None =>
                  httpClient.close()
                  spark.emptyDataFrame
              }
            case _ =>
              httpClient.close()
              throw new IllegalArgumentException(s"Option 'api_rest_method' not specified")

          }
        } else throw new IllegalArgumentException(s"Format not supported: $format. Formats supported: json.")
      case None => throw new IllegalArgumentException(s"Format is empty. Specify a format")
    }
  }

  private def authenticate(options: Map[String, String], httpClient: CloseableHttpClient): CloseableHttpResponse = {
    val result: Either[Error, Map[String, String]] = decode[Map[String, String]](options("api_rest_authentication_body"))
    val formParams = result match {
      case Right(map) =>
        map.map { case (key, value) =>
          new BasicNameValuePair(key, value)
        }.toList.asJava
      case Left(error) =>
        throw new IllegalArgumentException(s"Unable to parse api_rest_authentication_body: ${error.getMessage}")
    }
    val authRequest = new HttpPost(options("api_rest_authentication_host"))
    authRequest.setEntity(new UrlEncodedFormEntity(formParams))
    authRequest.addHeader("Content-Type", "application/x-www-form-urlencoded")
    httpClient.execute(authRequest)
  }
}

object RestApiStep {
  case class OauthAuthentication(access_token: String)
}
