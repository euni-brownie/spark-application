package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.example.Constants.*;

public class SparkApplication {


    public static void main(String[] args) {

        //spark common configuration without each property
        SparkConf sparkConf = new SparkConf()
                .setAppName("myApplication")
                .setMaster("local[*]")
//                .set("spark.driver.host","localhost")
                .set("spark.sql.caseSensitive", "false")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.sql.quotedIdentifiers", "false")
                .set("spark.sql.session.timeZone", "Asia/Seoul")
                .set("spark.hive.mapred.supports.subdirectories","true")
                .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true");
//                .set("spark.sql.hive.convertMetastoreParquet", "true");

        final String INPUT_PATH = sparkConf.get("spark.myapp.input");
        final String OUTPUT_PATH = sparkConf.get("spark.myapp.output");

        //spark session
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        //get csv input file to dataset
        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(INPUT_PATH);

        //from csv to parquet partition by kst timezone daily
        df.withColumn("event_time_kst", from_utc_timestamp(col("event_time"), "Asia/Seoul"))
                .withColumn("year", functions.year(col("event_time_kst")))
                .withColumn("month", functions.month(col("event_time_kst")))
                .withColumn("day", functions.dayofmonth(col("event_time_kst")))
                .write()
                .option("compression", "snappy")
                .mode("overwrite")
                .partitionBy("year","month","day")
                .parquet(OUTPUT_PATH);

        //create hive external table
        final String HIVE_SCHEMA = "(year int, month int, day int, event_time_kst int, event_time int, event_type string, product_id bigint, category_id bigint, category_code string, brand string, price double, user_id int, user_session string)";

        String dropExtTbSql = "DROP TABLE IF EXISTS " + TARGET_TABLE_NAME;
        String createExtTbSql = "CREATE external table "+ TARGET_TABLE_NAME + HIVE_SCHEMA
                + " PARTITIONED BY(year, month, day)"
                + " STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")"
                + " LOCATION '"+ OUTPUT_PATH +"'";

        spark.sql(dropExtTbSql);
        spark.sql(createExtTbSql);

        //show hive external table
        spark.sql("SELECT * FROM " + TARGET_TABLE_NAME).show(10);

        //TODO : save parquet using hive external table insert
/*
        String dropParquetTbSql = "DROP TABLE IF EXISTS " + PARQUET_TABLE_NAME;
        String createParquetTbSql = "CREATE table "+ PARQUET_TABLE_NAME + HIVE_SCHEMA
                + " STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")"
                + " location '" + OUTPUT_PATH + "'";

        String insertExtToParquetSql = "insert overwrite table "
                + PARQUET_TABLE_NAME + " select * from " + TARGET_TABLE_NAME;

        spark.sql(dropParquetTbSql);
        spark.sql(createParquetTbSql);
        spark.sql(insertExtToParquetSql);
*/

        spark.stop();
    }
}