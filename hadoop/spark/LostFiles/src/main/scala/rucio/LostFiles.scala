package rucio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, collect_list, concat_ws, lit, coalesce}
import org.apache.spark.sql.types.{TimestampType, LongType}
import org.apache.hadoop.fs.{FileSystem,Path}

object LostFiles {
    def main(args: Array[String]) {
        if (args.length != 4) {
            println("dump date, start, end date and report date have to be specified")
            System.exit(1)
        }

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"

        val date = args(0)
        val start = args(1).toLong
        val end = args(2).toLong
        val report_date = args(3)
        val spark = SparkSession.builder.appName("Rucio Lost Files").getOrCreate()

        spark.conf.set("spark.sql.session.timeZone", "UTC")
        import spark.implicits._
        val bad_replicas = spark
          .read
          .format("avro")
          .load("%s/%s/%s/bad_replicas".format(base_dir, dumps_dir, date))
          .select(
            $"scope",
            $"name",
            $"rse_id",
            $"state",
            $"account",
            $"updated_at".divide(1000).cast(LongType).as("updated_at"),
            $"created_at".divide(1000).cast(LongType).as("created_at")
          )
        .filter(
          ($"created_at" >= start) &&
          ($"created_at" < end) &&
          ($"state" === "L")
        )

        val contents_history = spark
          .read
          .format("avro")
          .load("%s/%s/%s/contents_history".format(base_dir, dumps_dir, date))
          .select(
            "scope",
            "name",
            "child_scope",
            "child_name",
            "did_type"
          )
          .filter($"did_type" === "D")
        
        val rses = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))
        
        val get_lost_files = bad_replicas.as("bad_reps")
          .join(
            contents_history.as("hist"),
            ($"bad_reps.scope" === $"hist.child_scope") &&
            ($"bad_reps.name" === $"hist.child_name")
          )
          .select(
            $"bad_reps.scope".as("fscope"),
            $"bad_reps.name".as("fname"),
            $"hist.scope",
            $"hist.name",
            $"bad_reps.rse_id",
            $"bad_reps.updated_at",
            $"bad_reps.account"
          )

        val output = get_lost_files.as("files")
          .join(
            rses.as("rses"),
            $"files.rse_id" === $"rses.id"
          )
          .select(
            "files.fscope",
            "files.fname",
            "files.scope",
            "files.name",
            "rses.rse",
            "files.account",
            "files.updated_at"
          )

        val output_path = "%s/%s/%s/lost_files.csv".format(base_dir, reports_dir, report_date)
        output
          .repartition(1)
          .write
          .option("delimiter", "\t")
          .csv(output_path)

        spark.stop()
    }
}