FROM eclipse-temurin:11-jdk-focal

RUN mkdir -p /opt/spooq/bin
RUN mkdir -p /opt/spooq/lib
RUN mkdir -p /opt/spooq/conf
RUN mkdir -p /opt/spooq/data

ADD https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz /opt

WORKDIR /opt
RUN tar xvzf spark-3.4.1-bin-hadoop3.tgz && rm spark-3.4.1-bin-hadoop3.tgz
# Scarica i file JAR da Maven e copiali nella cartella desiderata nel container
RUN wget -O /opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
RUN wget -O /opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.506.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.506/aws-java-sdk-bundle-1.12.506.jar
RUN wget -O /opt/spark-3.4.1-bin-hadoop3/jars/mssql-jdbc-12.4.2.jre11.jar https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar
RUN wget -O /opt/spark-3.4.1-bin-hadoop3/jars/ojdbc6-11.2.0.4.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc6/11.2.0.4/ojdbc6-11.2.0.4.jar
RUN wget -O /opt/spark-3.4.1-bin-hadoop3/jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
#WORKDIR /app/target/scala-2.12

ADD myDocker/bin/spooq /opt/spooq/bin
ADD myDocker/bin/loadenv /opt/spooq/bin
#ADD dist/lib/spooq.jar /opt/spooq/lib
COPY target/scala-2.12/*.jar /opt/spooq/lib/spooq.jar
ADD myDocker/log4j2.properties /opt/spooq
ADD start.sh /opt/spooq/start.sh

ENV SPOOQ_HOME=/opt/spooq

WORKDIR /opt/spooq

#ENTRYPOINT ["bin/spooq","-r"]
ENTRYPOINT ["/bin/bash", "./start.sh"]