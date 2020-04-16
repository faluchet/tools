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

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"
        val date = args(0)

        val spark = SparkSession.builder.appName("Rucio Consistency Datasets").getOrCreate()

        spark.conf.set("spark.sql.session.timeZone", "UTC")
        import spark.implicits._

        val dslocks = spark
          .read
          .format("avro")
          .load("%s/%s/%s/dslocks".format(base_dir, dumps_dir, date))
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
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))
        
        val dids = spark
          .read
          .format("avro")
          .load("%s/%s/%s/dids".format(base_dir, dumps_dir, date))
          .select(
            $"scope",
            $"name",
            coalesce($"bytes", lit(0)).as("bytes")
          )

        val join_reps_rses = dslocks.as("dslocks")
          .join(
            rses.as("rses"),
            $"dslocks.RSE_ID" === $"rses.ID"
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
            collect_list($"account").as("account"),
            max("bytes").as("bytes"),
            min("created_at").as("created_at"),
            max("accessed_at").as("accessed_at"),
            collect_list("rule_id").as("rule_id"),
            max("updated_at").as("updated_at")
          )

        val get_bytes = group_reps.as("group_reps")
          .join(
            dids.as("dids"),
            $"group_reps.scope" === $"dids.scope" &&
            $"group_reps.name" === $"dids.name"
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
            $"get_bytes.scope" === $"count_datasets.scope" &&
            $"get_bytes.name" === $"count_datasets.name"
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
            $"rse",
            $"rse".as("_rse"),
            $"scope",
            $"name",
            concat_ws(",", $"account"),
            $"bytes",
            $"created_at",
            $"accessed_at",concat_ws(",", $"rule_id"),
            $"count",
            $"updated_at"
          )

        val output_path = "%s/%s/%s/consistency_datasets".format(base_dir, reports_dir, date)
        get_output
            .repartition($"rse")
            .write
            .partitionBy("rse")
            .option("delimiter", "\t")
            .csv(output_path)

        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))

        // specific dump for Hammercloud testing. Only needed for ATLAS. Can be removed otherwise.
        val full_output = filter_okay
          .select("rse", "scope", "name")
          .filter(!$"name".like("%_dis%"))
          .filter(!$"name".like("%_sub%"))
          .orderBy(asc("rse"), asc("scope"), asc("name"))

        val full_output_path = "%s/%s/%s/consistency_datasets_full.bz2".format(base_dir, reports_dir, date)
        full_output
          .repartition(1)
          .write
          .option("delimiter", "\t")
          .option("compression","bzip2")
          .csv(full_output_path)

        spark.stop()
    }
}