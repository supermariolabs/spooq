package com.github.supermariolabs.spooq.udf.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, from_json, map_keys}
import org.apache.spark.sql.types.{MapType, StringType}

object UdfUtils {

  /**
   * parse a dataframe with a json array column to a new dataframe with every key as columns. The input df must be like:
   * +--------------------------------------------------------------------------------------------+
   *  |res                                                                                        |
   *  +-------------------------------------------------------------------------------------------+
   *  |[{"id": 1, "source": "email", "office_id": "office06"}]|
   *  +-------------------------------------------------------------------------------------------+
   *
   * with that general schema:
   *
   * root
   *  |-- res: array (nullable = true)
   *  |    |-- element: string (containsNull = true)
   *
   * @param df input dataframe
   * @param column column contaning json array
   * @return a new dataframe with json schema exploded
   */
  def parseJsonColumnToDfWithoutSchema(df: DataFrame, column: String): DataFrame = {
    val dfMapType = df.select(explode(col(column)).as("json"))
      .withColumn("json", from_json(col("json"), MapType(StringType, StringType)))
    //take all Map keys to generate columns
    val keysDF = dfMapType.limit(1).select(explode(map_keys(col("json"))))
    val keys = keysDF.collect().map(_.getString(0))
    val keyCols = keys.map(k => col("json").getItem(k).as(k))
    dfMapType.select(keyCols: _*)
  }

}
