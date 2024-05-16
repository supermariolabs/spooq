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

    val options = customInputStep.options.getOrElse(Map.empty)
    customInputStep.format match {
      case Some(format) =>
        if (customInputStep.format.get == "json") {
          val httpClient: CloseableHttpClient = HttpClients.createDefault()
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
                  val response = httpClient.execute(request)
                  response.getStatusLine.getStatusCode match {
                    case 200 =>
                      val entity = response.getEntity
                      val data = EntityUtils.toString(entity)
                      httpClient.close()
                      val stringDS = spark.createDataset(data.split("\n"))(Encoders.STRING)
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
