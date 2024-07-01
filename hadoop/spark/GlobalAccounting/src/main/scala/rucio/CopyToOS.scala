package rucio

import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, collect_list, concat_ws, lit, coalesce, when, sum, count, from_unixtime, to_timestamp}
import org.apache.spark.sql.types.{TimestampType, LongType, DoubleType, StringType, StructType}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.opensearch.spark._ 
import org.opensearch.spark.sql._
import org.apache.spark.sql.Row


object CopyToOS { 
    def main(args: Array[String]) {
        if (args.length != 2) {
            println("date and month have to be specified")
            System.exit(1)
        }

        val base_dir = "/user/rucio01"
        val reports_dir = "reports"
        val date = args(0)
        val month = args(1)

        val generate_id = udf((date: String, scope:String, name:String) => {
            val hstr = MessageDigest.getInstance("SHA1").digest(("%s_%s_%s".format(date, scope, name)).getBytes)
            (hstr.map("%02X" format _)).mkString
        })

        val get_datatype = udf((project: String, datatype: String) => {
            var tType = datatype
            if (tType == null) {
                tType = "no_name"
            }
            var tProject = project
            if (project == null) {
                tProject = ""
            }
            if (tType.startsWith("DAOD")) {
                tType = "DAOD"
            }
            if (tProject == "panda") {
                tType = "panda"
            }
            if (tProject.startsWith("user")) {
                tType = "user"
            }
            if (tProject.startsWith("archive")) {
                tType = "archive"
            }
            if (tProject.startsWith("data07")) {
                tType = "data07"
            }
            if (tProject.startsWith("sit")) {
                tType = "sit"
            }
            if (tProject == "hc_test") {
                tType = "hc_test"
            }
            if (tProject.startsWith("group")) {
                tType = "group"
            }
            if (tProject.startsWith("cond")) {
                tType = "cond"
            }
            if (tType.startsWith("HIST")) {
                tType = "HIST"
            }
            if (tType.startsWith("DESD")) {
                tType = "DESD"
            }
            if (tType.startsWith("DPD")) {
                tType = "DPD"
            }
            if (tType.startsWith("RDO")) {
                tType = "RDO"
            }
            if (tType.startsWith("NTUP")) {
                tType = "NTUP"
            }
            if (tType.startsWith("DRAW")) {
                tType = "DRAW"
            }
            if (tType.startsWith("DNTUP")) {
                tType = "DNTUP"
            }
            if (tType.startsWith("D2ESD")) {
                tType = "D2ESD"
            }
            if (tType.startsWith("D2AOD")) {
                tType = "D2AOD"
            }
            if (tType.endsWith("RAW")) {
                tType = "RAW"
            }
            if (tProject == "transient") {
                tType = "transient"
            }
            tType
        })

        val spark = SparkSession.builder.appName("Rucio Copy Global Accounting To ES").getOrCreate()

        spark.conf.set("spark.sql.session.timeZone", "UTC")

        import spark.implicits._

        val schema = new StructType()
          .add("scope",StringType,true)
          .add("name",StringType,true)
          .add("accessed_at",LongType,true)
          .add("created_at",LongType,true)
          .add("length",LongType,true)
          .add("bytes",LongType,true)
          .add("primary_files_t0",LongType,true)
          .add("primary_accessed_files_t0",LongType,true)
          .add("primary_files_t1",LongType,true)
          .add("primary_accessed_files_t1",LongType,true)
          .add("primary_files_t2",LongType,true)
          .add("primary_accessed_files_t2",LongType,true)
          .add("secondary_files_t0",LongType,true)
          .add("secondary_accessed_files_t0",LongType,true)
          .add("secondary_files_t1",LongType,true)
          .add("secondary_accessed_files_t1",LongType,true)
          .add("secondary_files_t2",LongType,true)
          .add("secondary_accessed_files_t2",LongType,true)
          .add("tape_files_t0",LongType,true)
          .add("tape_accessed_files_t0",LongType,true)
          .add("tape_files_t1",LongType,true)
          .add("tape_accessed_files_t1",LongType,true)
          .add("primary_bytes_t0",LongType,true)
          .add("primary_accessed_bytes_t0",LongType,true)
          .add("primary_bytes_t1",LongType,true)
          .add("primary_accessed_bytes_t1",LongType,true)
          .add("primary_bytes_t2",LongType,true)
          .add("primary_accessed_bytes_t2",LongType,true)
          .add("secondary_bytes_t0",LongType,true)
          .add("secondary_accessed_bytes_t0",LongType,true)
          .add("secondary_bytes_t1",LongType,true)
          .add("secondary_accessed_bytes_t1",LongType,true)
          .add("secondary_bytes_t2",LongType,true)
          .add("secondary_accessed_bytes_t2",LongType,true)
          .add("tape_bytes_t0",LongType,true)
          .add("tape_accessed_bytes_t0",LongType,true)
          .add("tape_bytes_t1",LongType,true)
          .add("tape_accessed_bytes_t1",LongType,true)
          .add("primary_repl_factor_t0",DoubleType,true)
          .add("primary_repl_factor_t1",DoubleType,true)
          .add("primary_repl_factor_t2",DoubleType,true)
          .add("secondary_repl_factor_t0",DoubleType,true)
          .add("secondary_repl_factor_t1",DoubleType,true)
          .add("secondary_repl_factor_t2",DoubleType,true)
          .add("tape_repl_factor_t0",DoubleType,true)
          .add("tape_repl_factor_t1",DoubleType,true)
          .add("datatype",StringType,true)
          .add("prod_step",StringType,true)
          .add("run_number",StringType,true)
          .add("project",StringType,true)
          .add("stream_name",StringType,true)
          .add("version",StringType,true)
          .add("campaign",StringType,true)
          .add("events",LongType,true)

        val df = spark
          .read
          .format("csv")
          .option("header", "false")
          .option("delimiter", "\t")
          .schema(schema)
          .load("%s/%s/%s/global_accounting".format(base_dir, reports_dir, date))
          .withColumn("total_files", $"primary_files_t0" + $"primary_files_t1" + $"primary_files_t2" + $"secondary_files_t0" + $"secondary_files_t1" + $"secondary_files_t2" + $"tape_files_t0" + $"tape_files_t1")
          .withColumn("total_accessed_files", $"primary_accessed_files_t0" + $"primary_accessed_files_t1" + $"primary_accessed_files_t2" + $"secondary_accessed_files_t0" + $"secondary_accessed_files_t1" + $"secondary_accessed_files_t2" + $"tape_accessed_files_t0" + $"tape_accessed_files_t1")
          .withColumn("disk_files", $"primary_files_t0" + $"primary_files_t1" + $"primary_files_t2" + $"secondary_files_t0" + $"secondary_files_t1" + $"secondary_files_t2")
          .withColumn("disk_accessed_files", $"primary_accessed_files_t0" + $"primary_accessed_files_t1" + $"primary_accessed_files_t2" + $"secondary_accessed_files_t0" + $"secondary_accessed_files_t1" + $"secondary_accessed_files_t2")
          .withColumn("primary_files", $"primary_files_t0" + $"primary_files_t1" + $"primary_files_t2")
          .withColumn("primary_accessed_files", $"primary_accessed_files_t0" + $"primary_accessed_files_t1" + $"primary_accessed_files_t2")
          .withColumn("secondary_files", $"secondary_files_t0" + $"secondary_files_t1" + $"secondary_files_t2")
          .withColumn("secondary_accessed_files", $"secondary_accessed_files_t0" + $"secondary_accessed_files_t1" + $"secondary_accessed_files_t2")
          .withColumn("tape_files", $"tape_files_t0" + $"tape_files_t1")
          .withColumn("tape_accessed_files", $"tape_accessed_files_t0" + $"tape_accessed_files_t1")
          .withColumn("total_bytes", $"primary_bytes_t0" + $"primary_bytes_t1" + $"primary_bytes_t2" + $"secondary_bytes_t0" + $"secondary_bytes_t1" + $"secondary_bytes_t2" + $"tape_bytes_t0" + $"tape_bytes_t1")
          .withColumn("total_accessed_bytes", $"primary_accessed_bytes_t0" + $"primary_accessed_bytes_t1" + $"primary_accessed_bytes_t2" + $"secondary_accessed_bytes_t0" + $"secondary_accessed_bytes_t1" + $"secondary_accessed_bytes_t2" + $"tape_accessed_bytes_t0" + $"tape_accessed_bytes_t1")
          .withColumn("disk_bytes", $"primary_bytes_t0" + $"primary_bytes_t1" + $"primary_bytes_t2" + $"secondary_bytes_t0" + $"secondary_bytes_t1" + $"secondary_bytes_t2")
          .withColumn("disk_accessed_bytes", $"primary_accessed_bytes_t0" + $"primary_accessed_bytes_t1" + $"primary_accessed_bytes_t2" + $"secondary_accessed_bytes_t0" + $"secondary_accessed_bytes_t1" + $"secondary_accessed_bytes_t2")
          .withColumn("primary_bytes", $"primary_bytes_t0" + $"primary_bytes_t1" + $"primary_bytes_t2")
          .withColumn("primary_accessed_bytes", $"primary_accessed_bytes_t0" + $"primary_accessed_bytes_t1" + $"primary_accessed_bytes_t2")
          .withColumn("secondary_bytes", $"secondary_bytes_t0" + $"secondary_bytes_t1" + $"secondary_bytes_t2")
          .withColumn("secondary_accessed_bytes", $"secondary_accessed_bytes_t0" + $"secondary_accessed_bytes_t1" + $"secondary_accessed_bytes_t2")
          .withColumn("tape_bytes", $"tape_bytes_t0" + $"tape_bytes_t1")
          .withColumn("tape_accessed_bytes", $"tape_accessed_bytes_t0" + $"tape_accessed_bytes_t1")
          .withColumn("primary_repl_factor", $"primary_repl_factor_t0" + $"primary_repl_factor_t1" + $"primary_repl_factor_t2")
          .withColumn("secondary_repl_factor", $"secondary_repl_factor_t0" + $"secondary_repl_factor_t1" + $"secondary_repl_factor_t2")
          .withColumn("tape_repl_factor", $"tape_repl_factor_t0" + $"tape_repl_factor_t1")
          .withColumn("timestamp", lit(date))
          .withColumn("id", generate_id($"timestamp", $"scope", $"name"))
          .withColumn("datatype_grouped", get_datatype($"project", $"datatype"))

        val osIndex = "atlas_ddm-global-accounting-" + month + "/_doc"
        df.saveToOpenSearch(osIndex)
    }
}
