#!/bin/bash
$SPARK_HOME/bin/spark-submit \
--class org.example.SparkApplication \
--master local \
--properties-file  conf/local.properties \
--conf "spark.driver.extraJavaOptions=-Duser.timezone=KST" \
--conf "spark.executor.extraJavaOptions=-Duser.timezone=KST" \
build/libs/data-engineering-test-1.0-SNAPSHOT.jar \
100

exit 0