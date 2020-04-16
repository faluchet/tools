package rucio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, when, collect_list, concat_ws, lit, coalesce}
import org.apache.spark.sql.types.TimestampType
import org.apache.hadoop.fs.{FileSystem,Path}

object DatasetsPerRSE { 
    def main(args: Array[String]) {
        if (args.length != 1) {
            println("date has to be specified")
            System.exit(1)
        }

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"
        val date = args(0)

        val spark = SparkSession.builder.appName("Rucio Dataset Per RSE").getOrCreate()
        import spark.implicits._
        spark.conf.set("spark.sql.session.timeZone", "UTC")

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

        val collection_replicas = spark
          .read
          .format("avro")
          .load("%s/%s/%s/collection_replicas".format(base_dir, dumps_dir, date))
          .select(
            "scope",
            "name",
            "rse_id",
            "did_type",
            "available_bytes",
            "state",
            "created_at",
            "accessed_at",
            "updated_at"
          )

        val rses = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))
        
        val filter_datasets = collection_replicas.filter("did_type = 'D'")

        val join_reps_locks = filter_datasets
          .as("reps")
          .join(
            dslocks.as("locks"),
            $"reps.scope" === $"locks.scope" &&
            $"reps.name" === $"locks.name" &&
            $"reps.rse_id" === $"locks.rse_id",
            "leftouter"
          )
          .select(
            $"reps.scope",
            $"reps.name",
            $"reps.rse_id",
            $"reps.available_bytes".as("bytes"),
            $"reps.state",
            $"reps.accessed_at".divide(1000).cast(TimestampType).as("accessed_at"),
            $"reps.updated_at".divide(1000).cast(TimestampType).as("updated_at"),
            $"reps.created_at".divide(1000).cast(TimestampType).as("created_at"),
            $"locks.account",
            $"locks.rule_id",
            $"locks.created_at".divide(1000).cast(TimestampType).as("ds_created_at")
          )

        val join_reps_rses = join_reps_locks.
          as("reps")
          .join(
            rses.as("rses"),
            $"reps.RSE_ID" === $"rses.ID"
          )
          .select(
            "rses.rse",
            "reps.scope",
            "reps.name",
            "reps.account",
            "reps.state",
            "reps.bytes",
            "reps.created_at",
            "reps.accessed_at",
            "reps.rule_id",
            "reps.updated_at",
            "reps.ds_created_at"
          )

        val group_reps = join_reps_rses
          .orderBy(asc("ds_created_at"))
          .groupBy("scope", "name", "rse")
          .agg(
            collect_list($"account").as("account"),
            max("bytes").as("bytes"),
            min("created_at").as("created_at"),
            max("updated_at").as("updated_at"),
            max("accessed_at").as("accessed_at"),
            collect_list("rule_id").as("rule_id"),
            max("state").as("state")
          )

        val get_output = group_reps
          .select(
            $"rse",
            $"rse".as("_rse"),
            $"scope",
            $"name",
            concat_ws(",", $"account").as("account"),
            $"bytes",
            $"created_at",
            $"updated_at",
            $"accessed_at",
            concat_ws(",", $"rule_id"),
            $"state"
          )
          .withColumn("account", when($"account" === "", "None").otherwise($"account"))
          .withColumn("bytes", when($"bytes".isNull, lit(0)).otherwise($"bytes"))
          .orderBy(
            asc("rse"),
            asc("scope"),
            asc("name")
          )

        val output_path = "%s/%s/%s/datasets_per_rse".format(base_dir, reports_dir, date)
        get_output
          .repartition($"rse")
          .write
          .partitionBy("rse")
          .option("delimiter", "\t")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
          .csv(output_path)

        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))

        spark.stop()
    }
}