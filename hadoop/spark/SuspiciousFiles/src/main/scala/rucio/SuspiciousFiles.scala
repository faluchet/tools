package rucio

import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, when, collect_list, concat_ws, lit, coalesce}
import org.apache.spark.sql.types.{TimestampType, LongType}
import org.apache.hadoop.fs.{FileSystem,Path}

object SuspiciousFiles {
    def main(args: Array[String]) {
        if (args.length != 4) {
            println("dump date, start and end date have to be specified")
            System.exit(1)
        }

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"
        val date = args(0)
        val start = args(1).toLong
        val end = args(2).toLong
        val report_date = args(3)
        
        val get_path = udf((prefix:String, scope:String, name:String) => {
            val hstr = MessageDigest.getInstance("MD5").digest(("%s:%s".format(scope, name)).getBytes)
            var fixed_scope = scope
            if (scope.startsWith("user") || scope.startsWith("group")) {
                fixed_scope = scope.replace(".", "/")
            }
            "%s/%s/%02x/%02x/%s".format(prefix, fixed_scope, hstr(0), hstr(1), name)
        })
        val add_prefix = udf((prefix:String, path:String) => {
            "%s%s".format(prefix, path)
        })
        val get_prefix = udf((scheme:String, hostname:String, prefix:String) => {
            "%s://%s%s".format(scheme, hostname, prefix)
        })
        val spark = SparkSession.builder.appName("Rucio Consistency Datasets").getOrCreate()

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
            $"created_at".divide(1000).cast(LongType).as("created_at")
          )
          .filter(
            ($"created_at" >= start) &&
            ($"created_at" < end) &&
            ($"state" === "S")
          )
        
        val replicas = spark
          .read
          .format("avro")
          .load("%s/%s/%s/replicas".format(base_dir, dumps_dir, date))
          .select(
            "scope",
            "name",
            "rse_id",
            "path"
          )
        
        val rses = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))
        
        val rse_protocols = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rse_protocols".format(base_dir, dumps_dir, date))
          .select(
            "scheme",
            "hostname",
            "prefix",
            "rse_id",
            "write_lan"
          )
          .filter($"write_lan" === "1")
        
        val with_prefix = bad_replicas.as("bad_reps")
          .join(
            rse_protocols.as("protos"),
            $"bad_reps.rse_id" === $"protos.rse_id"
          )
          .select(
            "protos.scheme",
            "protos.hostname",
            "protos.prefix",
            "bad_reps.scope",
            "bad_reps.name",
            "bad_reps.rse_id"
          )
          .withColumn("prefix", get_prefix($"scheme", $"hostname", $"prefix"))

        val with_path = with_prefix.as("with_prefix")
          .join(
            replicas.as("reps"),
            ($"with_prefix.scope" === $"reps.scope")
            && ($"with_prefix.name" === $"reps.name")
            && ($"with_prefix.rse_id" === $"reps.rse_id")
          )
          .select(
            $"with_prefix.rse_id".as("rse_id"),
            $"with_prefix.scope".as("scope"),
            $"with_prefix.name".as("name"),
            $"with_prefix.prefix".as("prefix"),
            $"reps.path".as("path")
          )
          .withColumn("path", when($"path".isNull, get_path($"prefix", $"scope", $"name")))
          .drop("prefix")

        val output = with_path.as("with_path")
          .join(
            rses.as("rses"),
            $"with_path.rse_id" === $"rses.id"
          )
          .select(
            "rses.rse",
            "with_path.scope",
            "with_path.name",
            "with_path.path"
          )
          .groupBy("rse", "scope", "name", "path")
          .count()
          .orderBy(asc("rse"), asc("scope"), asc("name"))
          .select(
            "rse",
            "scope",
            "name",
            "path"
          )

        val output_path = "%s/%s/%s/suspicious_files.csv".format(base_dir, reports_dir, report_date)
        output
          .repartition(1)
          .write
          .option("delimiter", "\t")
          .csv(output_path)

        spark.stop()
    }
}