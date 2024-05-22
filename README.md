![SPOOQ Logo](https://supermariolabs.github.io/spooq/docs/assets/images/banner.png "Title")

## Table of Contents
1. [Overview](#overview)
2. [Getting Started](#gettingstarted)
3. [Play with the tool using Docker](#docker)
4. [How does it work?](#howdoesitwork)
5. [Stream Processing](#streamprocessing)
6. [Interactive Mode](#interactivemode)
7. [Thrift Server (Experimental)](#thriftserver)
8. [Reference Documentation](#referencedoc)
   1. [Configuration Overview](#referencedoc1)
   2. [Steps Kind](#referencedoc2)
   3. [Launch Parameters](#referencedoc3)
9. [Rest API](#restapi)
10. [Download](#download)
11. [How to compile the code](howtocompile)
12. [Cookbook](#cookbook)

## Overview <a id="overview"></a>
Spooq is an ETL Big Data tool based on the Apache Spark framework that simplifies its use through the ability to implement data pipelines using a declarative approach based on simple configuration files and expressing transformations primarily through SQL.

The name is clearly inspired by the Apache Sqoop project, of which the tool is proposed as a replacement capable of exploiting the potential of Spark instead of the original tool's engine, which was instead based on Hadoop's MapReduce.

Spooq unlike its "predecessor" is capable, by leveraging the capabilities of Apache Spark, of:
- Supporting a large number of data sources in batch and streaming mode (e.g.: datasets stored on HDFS, S3, ADLS Gen2 and GCS in CSV, Parquet, Avro and other formats as well as RDBMS via JDBC or all popular NoSQL, Apache Kafka and more);
- Allowing to implement complex data pipelines by leveraging Spark SQL's built-in language and functions and using UDFs and simple custom code blocks where necessary;
- Use an interactive mode to debug flows and explore data easily and intuitively;
- Other features that will be better explained in the documentation.

## Getting Started <a id="gettingstarted"></a>
For the impatient ones, here is a quick guide to get started with the tool quickly. In keeping with tradition we start with the classic "hello world" example (don't worry if it doesn't make much sense from a functional point of view, it serves to understand the basic philosophy of the tool).

The tool takes as input a configuration file (HOCON, JSON or YAML format can be used) with some identifying information about the flow and a sequence of processing `steps`. For example, using the following configuration:
```hocon
id = "helloWorld"
desc = "sample 'hello world' job"

steps = [
{
    id = hello
    shortDesc = "execute 'hello world' sql query"
    kind = sql
    sql = "select 'hello world!' as message"
    show = true
}
]
```
Launching the application we will get the following output:
![Asciinema](https://supermariolabs.github.io/spooq/docs/assets/images/spooq1.gif "Spooq: 'hello world'!'")
[Watch in Asciinema](https://asciinema.org/a/522289)

Okay, now let's try to do something more useful. For example, a CSV format import of a Postgres table by connecting via JDBC:
```hocon
id = "sample job"
desc = "sample spooq job that ingest data from database table through jdbc connection"

steps = [
{
    id = customers
    shortDesc = "load from jdbc"
    desc = "load 'customer' table from postgres database"
    kind = input
    format = jdbc
    options = {
        url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/wllbjgnv"
        driver = "org.postgresql.Driver"
	dbtable = "public.customer"
        user = "wllbjgnv"
        password = "**********"
        numPartitions = "1"
    }
    cache = true
    show = true
},
{
      id = out
      shortDesc = "write to fs"
      dependsOn = ["customers"]
      desc = "write 'customer' table to fs using csv format"
      kind = output
      source = customers
      format = csv
      options = {
        header = "true"
      }
      mode = overwrite
      path = "/tmp/customer.csv"
},
]
```
*Please note:* in order to connect via jdbc to Postgres we must have the connector available in the classpath. We will explain this step in more detail later.

Let us now run the tool with the above configuration file:
![Asciinema](https://supermariolabs.github.io/spooq/docs/assets/images/spooq2.gif "Spooq: jdbc import'")
[Watch in Asciinema](https://asciinema.org/a/522300)

Let's try adding a data preparation step (we can use as many as we need):
```hocon
id = "sample job"
desc = "sample spooq job that ingest data from database table through jdbc connection"
steps = [
{
    id = customers
    shortDesc = "load from jdbc"
    desc = "load 'customer' table from postgres database"
    kind = input
    format = jdbc
    options = {
        url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/wllbjgnv"
        driver = "org.postgresql.Driver"
	dbtable = "public.customer"
        user = "wllbjgnv"
        password = "**********"
        numPartitions = "1"
    }
    cache = true
},
{
    id = prepared
    shortDesc = "filter and prepare output"
    kind = sql
    sql = "select customer_id,first_name,last_name,email from customers"
    show = true
},
{
      id = out
      shortDesc = "write to fs"
      desc = "write 'customer' table to fs using csv format"
      kind = output
      source = prepared
      format = parquet
      mode = overwrite
      path = "/tmp/customer.parquet"
},
]
```

Let's launch the tool again using the new configuration:
![Asciinema](https://supermariolabs.github.io/spooq/docs/assets/images/spooq3.gif "Spooq: jdbc import 2'")
[Watch in Asciinema](https://asciinema.org/a/522363)

## Play with the tool using Docker <a id="docker"></a>
The best way to start testing the tool is to use the prepackaged Docker image by following these simple steps:
1. In a folder of your choice create two subfolders named: `conf` and `data`;
2. Inside the conf folder create a hello.conf file with the following content
```hocon
id = "helloWorld"
desc = "sample 'hello world' job"

steps = [
{
    id = hello
    shortDesc = "execute 'hello world' sql query"
    kind = sql
    sql = "select 'hello world!' as message"
    show = true
},
{
    id = out
    shortDesc = "sample output"
    kind = output
    source = hello
    format = json
    mode = overwrite
    path = /opt/spooq/data/hello.json
}
]
```

3. Launch the docker image with the following command:
```bash
docker run -v $(pwd)/conf:/opt/spooq/conf -v $(pwd)/data:/opt/spooq/data -it mcartia/spooq -c conf/hello.conf
```

4. If everything went smoothly you should find the job output in the `data/hello.json/` directory. Congratulations!

**Please note**:
You can pass any arguments supported by spooq. The application will run locally on a standalone Spark installation embedded in the docker image.

It is possible to load additional dependencies by setting the SPOOQ_PACKAGES environment variable. For example, if we wanted to launch the application by loading the jdbc postgres connector it would be enough to use:
```bash
docker run -v $(pwd)/conf:/opt/spooq/conf -v $(pwd)/data:/opt/spooq/data -e SPOOQ_PACKAGES=org.postgresql:postgresql:42.4.0 -it mcartia/spooq -c conf/your.conf
```
(to load multiple dependencies just separate them with a comma `,`)

## How does it work? <a id="howdoesitwork"></a>
The tool is nothing more than a generic Spark application that can be launched in standalone mode (`master=local[*]` via a fat-jar containing within it the Apache Spark framework and other libraries) or on a cluster via `spark-submit`.

It is also possible to use the tool as a library within other spark applications or from spark-shell:
![Asciinema](https://supermariolabs.github.io/spooq/docs/assets/images/spooq4.gif "Spooq: spark-shell")
[Watch in Asciinema](https://asciinema.org/a/mjoaU6GgjhMb1b3dR0flJNP57)

In this mode we will receive as output an object with all the dataframes and variables created during the execution of the job defined in the configuration used so we can continue to process it easily interactively in the REPL.

The downloadable distribution also contains sample launch scripts in the various modes. This is the ones that make use of `spark-submit`:
```bash
#!/bin/bash
source $SPOOQ_HOME/bin/loadenv

JAR=$SPOOQ_HOME/lib/spooq-spark3.jar
MAIN=com.github.supermariolabs.spooq.Application

# Example
# SPOOQ_PACKAGES=org.postgresql:postgresql:42.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0

if [ -n "$SPOOQ_PACKAGES" ]
then
    echo "PACKAGES_CMD=--packages $SPOOQ_PACKAGES"
    PACKAGES_CMD="--packages $SPOOQ_PACKAGES"
else
    PACKAGES_CMD=
fi

ARGS="$@"

if [ -z "$JAVA_HOME" ]
then
    echo "JAVA_HOME not defined!"
else
    echo "Using JAVA_HOME=$JAVA_HOME"
fi

if [ -z "$SPARK_HOME" ]
then
    echo "SPARK_HOME not defined!"
else
    echo "Using SPARK_HOME=$SPARK_HOME"
    $SPARK_HOME/bin/spark-submit \
	--class $MAIN \
	--master local[*] \
	--conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=$SPOOQ_HOME/conf/log4j2.properties \
	--conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=$SPOOQ_HOME/conf/log4j2.properties \
	$PACKAGES_CMD \
	$JAR \
	$ARGS
fi
```

As you can see, you can use the system to load dependencies from maven-compatible repositories using the `--packages` (and `--repositories`) option of the `spark-submit` command!

## Stream Processing <a id="streamprocessing"></a>
The framework also supports the Structured Streaming API via step blocks with `input-stream` and `output-stream` kind. Let us look at an example and also introduce the use of UDFs to enrich the already large collection of built-in SQL functions provided by Spark:
```hocon
id = "sample streaming job"

steps = [
{
    id = stream
    shortDesc = "generate fake stream"
    kind = input-stream
    format = rate
    options = {
        rowsPerSecond = "2"
    }
},
{
    id = randomCustomer
    shortDesc = "load sample udf"
    kind = udf
    claz = com.github.supermariolabs.spooq.udf.example.FakeCustomerUDF
},
{
    id = enriched
    shortDesc = "enrich stream using sql and udf"
    kind = sql
    sql = "select customer.* from (select randomCustomer(value) customer from stream)"
},
{
    id = outStream
    source = enriched
    shortDesc = "stream dump"
    kind = output-stream
    format = console
    outputMode = "append"
    trigger = {
                policy = "processingTime"
                value = "10 seconds"
    }
}
]
```

Which once launched will produce:
![Asciinema](https://supermariolabs.github.io/spooq/docs/assets/images/spooq5.gif "Spooq: streaming 1")
[Watch in Asciinema](https://asciinema.org/a/522379)

## Interactive Mode <a id="interactivemode"></a>
Interactive mode allows you to be able to execute SQL queries once the pipeline execution is finished. This mode is very useful during pipeline development and debugging as well as for being able to perform interactive analysis of data from multiple sources using Spark's distributed SQL engine.

This mode is triggered by the use of the `--interactive` (or `-i`) switch when launching the application. Let's use this simple configuration:
```hocon
id = "sample job"
steps = [
{
    id = customers
    shortDesc = "load customer.csv file"
    kind = input
    format = csv
    schema = "customer_id int ,store_id int ,first_name string,last_name string,email string,address_id string,activebool string,create_date string,last_update string,active string"
    options = {
        header = "true"
    }
    path = "/tmp/customer.csv"
    cache = true
}]
```
we will get:
![DBeaver Community](https://supermariolabs.github.io/spooq/docs/assets/images/spooq6.gif "DBeaver Community")
[Watch in Asciinema](https://asciinema.org/a/522550)

## Thrift Server (Experimental) <a id="thriftserver"></a>
An experimental feature allows a built-in thrift server to be started (on a configurable port, default: 10001) and use a client via the standard Hive JDBC driver to be able to make SQL queries on the views (temporary tables) created during processing.

For example, you can use the opensource [DBeaver Community](https://dbeaver.io) client:
![DBeaver](https://supermariolabs.github.io/spooq/docs/assets/images/spooq7.png "DBeaver")

## Reference Documentation <a id="referencedoc"></a>
### Configuration Overview <a id="referencedoc1"></a>
Configuration files can be written in [HOCON](https://en.wikipedia.org/wiki/HOCON), [JSON](https://en.wikipedia.org/wiki/JSON) or [YAML](https://en.wikipedia.org/wiki/YAML) format. Decoding is implemented through the use of the [Circe](https://circe.github.io/circe/) library. When the format is not specified as a launch parameter, the application tries to infer it from the file extension.

All configuration blocks (including the root) have an identifier (`id`, mandatory) a short description (`shortDesc`, optional) and an extended description (`desc`, optional) and a list of `steps` (mandatory but can be empty). The following is an example of a valid minimal configuration in HOCON format:
```hocon
id = minimal
shortDesc = "Minimal configuration example"
desc = "This example configuration does nothing but is formally valid"
steps = []
```

Which will produce:
![Minimal configuration](https://supermariolabs.github.io/spooq/docs/assets/images/spooq8.png "minimal configuration")

### Steps Kind <a id="referencedoc2"></a>
#### input
A step with kind `input` loads a `DataFrame` starting from a [data source natively supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources.html) or using a third-party connector.

For each `DataFrame` a corresponding _temporary view_ is also created whose name is the `id` of the corresponding `step` block. In this way it will be possible to reference the same in subsequent blocks within SQL queries.

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `format` data source format
- `options` data source options
- `schema` schema in Spark SQL DDL format
- `path` data source path (optional)
- `cache` whether to apply dataframe caching (N.B. lazy as default on Spark)
- `show` whether to display a diagnostic dataframe data sample
- `isGlobal` whether to create a global temp view instead a normal temp view (deafault is false)

Examples:
```hocon
steps = [
   {
      id = customers
      shortDesc = "load customer.csv file"
      kind = input
      format = csv
      schema = "customer_id int ,store_id int ,first_name string,last_name string,email string,address_id string,activebool string,create_date string,last_update string,active string"
      options = {
         header = "true"
      }
      path = "/tmp/customer.csv"
      cache = true
      show = true
   },
   #...
   {
      id = jdbc
      shortDesc = "load from jdbc"
      desc = "load 'customer' table from postgres database"
      kind = input
      format = jdbc
      options = {
         url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/wllbjgnv"
         driver = "org.postgresql.Driver"
         dbtable = "public.customer"
         user = "wllbjgnv"
         password = "F-pOL8v410XRmLrC43PCKlazY_-cT11k"
         numPartitions = "1"
      }
      cache = true
      show = true
   }
]
```
#### customInput
A step with kind `customInput` creates a dataframe and the corresponding _temporary view_ executing custom code defined in a class that extends the `customInputStep` trait:
```scala
package com.github.supermariolabs.spooq.etl

import com.github.supermariolabs.spooq.model.Step
import org.apache.spark.sql.DataFrame

trait CustomInputStep extends Serializable {
  def run(dfMap: Map[String, DataFrame], variables: Map[String, Any], args : Map[String,String], customInputStep : Step): DataFrame
}

```

- `format` data source format
- `options` data source options
- `schema` schema in Spark SQL DDL format
- `path` data source path (optional)
- `claz` class name including package
- `cache` whether to apply dataframe caching (N.B. lazy as default on Spark)
- `show` whether to display a diagnostic dataframe data sample
- `isGlobal` whether to create a global temp view instead a normal temp view (deafault is false)

In our example we developed a custom input step that reads from api response json authenticated by Oauth (experimental):
```hocon
steps = [
{
    id = exampleRestSource
    shortDesc = "load a json response from a REST api"
    kind = customInput
    format = json
    options = {
        api_rest_authentication_body = "{\"client_id\": \"12345678\",\"client_secret\": \"12345678\",\"grant_type\": \"client_credentials\"}",
        api_rest_method = "POST",
        multiline = "true",
        header = "true",
        api_rest_body = "{\"flowDate\": \"2022-04-04\",\"hours\": [14,15],\"zones\": [\"CNOR\",\"CSUD\"],\"statuses\": [\"null\",\"REP\",\"SENT_OK\"]}",
        api_rest_authentication_host = "https://exampleHost.com/oauth/token"
    }
    claz = com.github.supermariolabs.spooq.etl.RestApiStep
    path = "https://exampleHostToDoTheCallTo.com/examplePath"
    cache = true
    show = true
},
#...
```
#### input-stream
A step with kind `input-stream` loads a (streaming) `DataFrame` starting from a data source natively supported by Spark or using a third-party connector. The feature uses [Spark's Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) API.

For each (streaming) `DataFrame` a corresponding _temporary view_ is also created whose name is the `id` of the corresponding `step` block. In this way it will be possible to reference the same in subsequent blocks within SQL queries.

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `format` data source format
- `options` data source options
- `schema` schema in Spark SQL DDL format
- `avro` **experimental** decode the value of a message (from a kafka topic)
- `avroSchema` **experimental** the avro schema that will be used for the above decoding
- `path` data source path (optional)
- `show` whether to display a diagnostic dataframe for each triggered batch

Examples:
```hocon
steps = [
{
    id = stream
    shortDesc = "generate fake stream"
    kind = input-stream
    format = rate
    options = {
        rowsPerSecond = "2"
    }
},
#...
   {
      id = kafka
      shortDesc = "kafka topic input"
      kind = input-stream
      format = kafka
      options = {
         "kafka.bootstrap.servers" = "localhost:9092"
         subscribe = "spooq"
         includeHeaders = "true"
      }
   }
]
```
#### sql
A step with kind `sql` creates a dataframe (in batch or streaming mode) and the corresponding _temporary view_ starting from an SQL query that makes use of dataframes previously created in other blocks (`input`, `input-stream` or other `sql` blocks).

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `sql` the transformation query you want to perform
- `cache` whether to apply dataframe caching (N.B. lazy as default on Spark)
- `show` whether to display a diagnostic dataframe data sample

Examples:
```hocon
steps = [
{
    id = customers
    shortDesc = "load customer.csv file"
    kind = input
    format = csv
    options = {
        header = "true"
    }
    path = "/tmp/customer.csv"
    cache = true
    show = true
},
{
    id = filter
    shortDesc = "filter customers"
    kind = sql
    sql = "select * from customers where substr(first_name,0,1)='B'"
    show = true
},
{
    id = prepared
    shortDesc = "prepare output"
    kind = sql
    sql = "select first_name, upper(last_name), email from filter"
    show = true
},
#...
```
#### variable
A step with kind `variable` creates a variable that is put into the variables map and can be referenced in sql blocks through placeholders.

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `sql` the query you want to use to populate the value (it can return multiple values, in this case the variable will be a list)

Examples:
```hocon
steps = [
  {
    id = helloVar
    kind = variable
    sql = "select 'hello world!'"
    show = true
  },
  {
    id = hello
    shortDesc = "execute hello world sql query"
    kind = sql
    sql = """select '#{variables.helloVar}' as message"""
    show = true
  },
#...
```
#### script (experimental)
A step with kind `script` creates a dataframe and the corresponding _temporary view_ evaluating a script interpreted by a [JSR 223](https://jcp.org/en/jsr/detail?id=223) engine.

Inside the snippet we will be able to use the variables `sc` (SparkContext), `spark` (SparkSession), `logger` (Logger slf4j), all the _dataframes_ and _variables created_ in the previous blocks (referenced using the `id` as a name).

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `jsr223Engine` the engine you want to use (default: `scala`)
- `code` the code in the chosen language
  Examples:
```hocon
steps = [
{
    id = hello
    shortDesc = "execute 'hello world' sql query"
    kind = sql
    sql = "select 'hello world!' as message"
    show = true
},
{
    id = scalaTest
    kind = script
    jsr223Engine = scala
    code = """
           //scala code example
           logger.info("scala script example...")

           def helloMsg(): String = {
                "Hello, SCALA world!"
                }
           
           hello.union(spark.sql(s"select '${helloMsg()}' as hello"))
           """
},
{
    id = jsTest
    kind = script
    jsr223Engine = js
    code = """
           //js code example
           logger.info("js script example...")

           function helloMsg() {
                 return "Hello, JS world!";
           }

           hello.union(spark.sql("select '"+helloMsg()+"' as hello"))
           """
},
#...
```
#### custom
A step with kind `custom` creates a dataframe and the corresponding _temporary view_ executing custom code defined in a class that extends the `SimpleStep` trait:
```scala
package com.github.supermariolabs.spooq.etl

import org.apache.spark.sql.DataFrame

trait SimpleStep extends Serializable {
  def run(dfMap: Map[String, DataFrame], variables: Map[String, Any]): DataFrame
}
```

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `claz` class name including package
- `cache` whether to apply dataframe caching (N.B. lazy as default on Spark)
- `show` whether to display a diagnostic dataframe data sample

  Examples:
```hocon
steps = [
{
    id = customers
    shortDesc = "load customer.csv file"
    kind = input
    format = csv
    schema = "customer_id int ,store_id int ,first_name string,last_name string,email string,address_id string,activebool string,create_date string,last_update string,active string"
    options = {
        header = "true"
    }
    path = "/tmp/customer.csv"
    cache = true
    show = true
},
{
    id = custom
    shortDesc = "custom step"
    kind = custom
    claz = com.github.supermariolabs.spooq.etl.SampleStep
},
#...
```
#### udf
A step with kind `udf` registers a custom user defined function that can be used inside subsequent `sql` blocks.

The function must be implemented (and available in the classpath at runtime) by extending the `SimpleUDF` trait:
```scala
package com.github.supermariolabs.spooq.udf

import org.apache.spark.sql.expressions.UserDefinedFunction

trait SimpleUDF extends Serializable {
   val udf: UserDefinedFunction
}
```

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `claz` class name including package

Examples:
```hocon
steps = [
{
    id = stream
    shortDesc = "generate fake stream"
    kind = input-stream
    format = rate
    options = {
        rowsPerSecond = "2"
    }
},
{
    id = randomCustomer
    shortDesc = "load sample udf"
    kind = udf
    claz = com.github.supermariolabs.spooq.udf.example.FakeCustomerUDF
},
{
    id = enriched
    shortDesc = "enrich stream using sql and udf"
    kind = sql
    sql = "select customer.* from (select randomCustomer(value) customer from stream)"
},
#...
```
#### output
A step with kind `output` writes a `DataFrame` to any [data source natively supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources.html) or using a third-party connector.

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:
- `format` data source format
- `options` data source options
- `path` data source path (optional)
- `mode` [save mode](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes)
- `partitionBy` partitioning column(s)

Examples:
```hocon
steps = [
{
    id = customers
    shortDesc = "load customer.csv file"
    kind = input
    format = csv
    schema = "customer_id int ,store_id int ,first_name string,last_name string,email string,address_id string,activebool string,create_date string,last_update string,active string"
    path = "/tmp/customer.csv"
    cache = true
    show = true
},
{
    id = filter
    shortDesc = "filter customers"
    kind = sql
    sql = "select * from customers where substr(first_name,0,1)='B'"
    show = true
},
{
    id = prepared
    shortDesc = "prepare output"
    kind = sql
    sql = "select first_name, upper(last_name), email from filter"
    show = true
},
{
      id = out
      shortDesc = "write to fs"
      dependsOn = ["filter"]
      desc = "write filtered customers table to fs using json format"
      kind = output
      source = prepared
      format = json
      mode = overwrite
      path = "/tmp/customer.json"
},
#...
```
#### output-stream
A step with kind `output-stream` creates a `streaming DataFrame` that writes to a data source natively supported by Spark or using a third-party connector. The feature uses [Spark's Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) API.

The properties supported by this type of block (in addition to the common ones `id`, `shortDesc` and `desc`) are:

- `format` data source format
- `options` data source options
- `path` data source path (optional)
- `partitionBy` partitioning column(s)
- `outputMode` see [output modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes)
- `trigger` see [triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)

Examples:
```hocon
steps = [
{
    id = stream
    shortDesc = "generate fake stream"
    kind = input-stream
    format = rate
    options = {
        rowsPerSecond = "2"
    }
},
{
    id = randomCustomer
    shortDesc = "load sample udf"
    kind = udf
    claz = com.github.supermariolabs.spooq.udf.example.FakeCustomerUDF
},
{
    id = enriched
    shortDesc = "enrich stream using sql and udf"
    kind = sql
    sql = "select customer.* from (select randomCustomer(value) customer from stream)"
},
{
    id = outStream
    source = enriched
    shortDesc = "stream dump"
    kind = output-stream
    format = console
    outputMode = "append"
    trigger = {
                policy = "processingTime"
                value = "10 seconds"
    }
},
#...
```
### Launch Parameters <a id="referencedoc3"></a>
The command parameters supported by the application are:
- `-c` or `--configuration-file` configuration file to use (default: spooq.conf)
- `-f` or `--format` configuration file format (hocon, conf, yaml, yml, json, default is inferred from filename extension) 
- `-i` or `--interactive` interactive mode, after performing the steps defined in the configuration file opens a repl shell where you can execute sql commands and more
- `-ts` or `--thrift-server` (**experimental**) starts a thrift server on a port (default: 10001) for connection through third party clients
- `-tp` or `--thrift-port` (**experimental**) allows you to bind the thrift-server to a different port
- `-v` or `--verbose` verbose mode
- `-r` or `--rich` whether or not to use the rich ui (ansi) cli

## Rest API <a id="restapi"></a>
You can interact with spooq also with rest api (remember to pass --http option when tou launch spooq):
Example:
```bash
./spooq/bin/spooq -c 'spooq/conf/example.conf' --http
```
Below are all the Rest APIs, with examples of how to call them  (4242 is the default port the service is listening to):

### Add a step
```bash
curl -X POST localhost:4242/step
```

request body (json):
```json
{
      "id":"orders_spooq",
      "desc":"load orders_spooq.csv file",
      "kind":"input",
      "format":"csv",
      "options": {
         "header":"true"
      },
      "path":"s3a://test/poc/orders_spooq.csv",
      "cache":true
}
```

### Cache a dataframe
```bash
curl -X GET localhost:4242/cache/{dataframeName}
```

path variable:
```bash
dataframeName: name of the dataframe you want to cache
```

### Unpersist a dataframe
```bash
curl -X GET localhost:4242/unpersist/{dataframeName}
```

path variable:
```bash
dataframeName: name of the dataframe you want to unpersist
```

### Delete a dataframe
```bash
curl -X DELETE localhost:4242/step/{dataframeName}
```

path variable:
```bash
dataframeName: name of the dataframe you want to delete
```

## Download <a id="download"></a>
[Spooq 0.9.9beta Spark3 (scala 2.12)](https://drive.google.com/file/d/1y57-YBmEyfLhxI2Rw4Rh3QOBeLAiOcah/view?usp=sharing)

[Spooq 0.9.9beta Spark3 (scala 2.12) standalone](https://drive.google.com/file/d/1uIAP_6D_tuVU5WrEscVDwE6bj1lewQXp/view?usp=sharing)

[Spooq 0.9.9beta Spark2 (scala 2.11)](https://drive.google.com/file/d/183PFqWqih5O2KR-hL74Kyq925-ocveoe/view?usp=sharing)

[Spooq 0.9.9beta Spark2 (scala 2.11) standalone](https://drive.google.com/file/d/1pt01Gkx75xGuC53Du5IkyagZ1E9Pp7_C/view?usp=sharing)

## How to compile the code <a id="howtocompile"></a>
To compile and create a custom build you can use [sbt](https://www.scala-sbt.org). In particular it is possible to compile the software with `Scala 2.11` for `Spark 2.x` and with `Scala 2.12` for `Spark 3.x`.

It is also possible to build the software _with_ or _without_ the Spark embedded libraries (depending on whether you want to use it _standalone_ or on a cluster with `spark-submit`).

Examples:
```bash
# compile using Scala 2.11/Spark 2.x building with spark provided dependencies
$ sbt -Dbuild.spark.version=2 configString assembly

# compile using Scala 2.12/Spark 3.x building fat jar (standalone)
$ sbt -Dbuild.spark.version=3 -Dstandalone=true configString assembly

...
```

## Cookbook <a id="cookbook"></a>
TODO