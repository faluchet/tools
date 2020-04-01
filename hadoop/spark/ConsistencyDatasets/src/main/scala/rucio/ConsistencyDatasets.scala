package rucio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, collect_list, concat_ws, lit, coalesce}
import org.apache.hadoop.fs.{FileSystem,Path}

object ConsistencyDatasets { 
    def main(args: Array[String]) {
        if (args.length != 1) {
            println("date has to be specified")
            System.exit(1)
        }

        val date = args(0)

        val spark = SparkSession.builder.appName("Rucio Consistency Datasets").getOrCreate()

        spark.conf.set("spark.sql.session.timeZone", "UTC")
        import spark.implicits._

        val dslocks = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/dslocks")
          .select(
            "scope",
            "name",
            "rse_id",
            "account",
            "state",
            "bytes",
            "created_at",
            "accessed_at",
            "rule_id",
            "updated_at"
          )
        
        val rses = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/rses")
        
        val dids = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/dids")
          .select(
            col("scope"),
            col("name"),
            coalesce(col("bytes"),
            lit(0)).as("bytes")
          )

        val join_reps_rses = dslocks.as("dslocks")
          .join(
            rses.as("rses"),
            col("dslocks.RSE_ID") === col("rses.ID")
          )
          .select(
            "rses.rse",
            "dslocks.scope",
            "dslocks.name",
            "dslocks.account",
            "dslocks.state",
            "dslocks.bytes",
            "dslocks.created_at",
            "dslocks.accessed_at",
            "dslocks.rule_id",
            "dslocks.updated_at"
          )

        val filter_okay = join_reps_rses.filter("state = 'O'")

        val group_reps = filter_okay
          .orderBy(asc("created_at"))
          .groupBy("scope", "name", "rse")
          .agg(
            collect_list(col("account")).as("account"),
            max("bytes").as("bytes"),
            min("created_at").as("created_at"),
            max("accessed_at").as("accessed_at"),
            collect_list("rule_id").as("rule_id"),
            max("updated_at").as("updated_at")
          )

        val get_bytes = group_reps.as("group_reps")
          .join(
            dids.as("dids"),
            (col("group_reps.scope") === col("dids.scope")) && (col("group_reps.name") === col("dids.name"))
          )
          .select(
            "group_reps.rse",
            "group_reps.scope",
            "group_reps.name",
            "group_reps.account",
            "dids.bytes",
            "group_reps.created_at",
            "group_reps.accessed_at",
            "group_reps.rule_id",
            "group_reps.updated_at"
          )

        val count_datasets = filter_okay
          .groupBy("scope", "name", "rse")
          .count()
          .groupBy("scope", "name")
          .count()

        val join_reps_all = get_bytes
          .as("get_bytes")
          .join(
            count_datasets.as("count_datasets"),
            (col("get_bytes.scope") === col("count_datasets.scope")) && (col("get_bytes.name") === col("count_datasets.name"))
          )
          .select(
            "get_bytes.rse",
            "get_bytes.scope",
            "get_bytes.name",
            "get_bytes.account",
            "get_bytes.bytes",
            "get_bytes.created_at",
            "get_bytes.accessed_at",
            "get_bytes.rule_id",
            "count_datasets.count",
            "get_bytes.updated_at"
          )

        val get_output = join_reps_all
          .select(
            col("rse"),
            col("rse").as("_rse"),
            col("scope"),
            col("name"),
            concat_ws(",", col("account")),
            col("bytes"),
            col("created_at"),
            col("accessed_at"),concat_ws(",", col("rule_id")),
            col("count"),
            col("updated_at")
          )

        val output_path = "/user/rucio01/tmp/" + date + "/consistency_datasets"
        get_output
            .repartition($"rse")
            .write
            .partitionBy("rse")
            .mode("overwrite")
            .option("delimiter", "\t")
            .csv(output_path)

        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))

        val full_output = filter_okay
          .select("rse", "scope", "name")
          .filter(!col("name").like("%_dis%"))
          .filter(!col("name").like("%_sub%"))
          .orderBy(asc("rse"), asc("scope"), asc("name"))

        full_output
          .repartition(1)
          .write
          .mode("overwrite")
          .option("delimiter", "\t")
          .option("compression","bzip2")
          .csv("/user/rucio01/tmp/" + date + "/consistency_datasets_full.bz2")

        spark.stop()
    }
}