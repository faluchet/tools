package rucio

import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, when, collect_list, concat_ws, lit, coalesce}
import org.apache.spark.sql.types.TimestampType
import org.apache.hadoop.fs.{FileSystem,Path}

object DatasetsPerRSE { 
    def main(args: Array[String]) {
        if (args.length != 3) {
            println("dump date, start and end date have to be specified")
            System.exit(1)
        }

        val date = args(0)
        val start = args(1)
        val end = args(2)
        
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
          .load("/user/rucio01/dumps/" + date + "/bad_replicas")
          .select(
            $"scope",
            $"name",
            $"rse_id",
            $"state",
            $"created_at".divide(1000)
          )
          .filter($"created_at" >= start)
          .filter($"created_at" < end)
          .filter("state = 'S'")
        
        val replicas = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/replicas")
          .select(
            "scope",
            "name",
            "rse_id",
            "path"
          )
        
        val rses = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/rses")
        
        val rse_protocols = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/rse_protocols")
          .select(
            "scheme",
            "hostname",
            "prefix",
            "rse_id",
            "write_lan"
          )
          .filter("write_lan = '1'")
        
        val with_prefix = bad_replicas.as("bad_reps")
          .join(rse_protocols.as("protos"), $"bad_reps.rse_id" === $"protos.rse_id")
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
            ($"with_prefix.scope" === $"reps.scope") && ($"with_prefix.name" === $"reps.name") && ($"with_prefix.rse_id" === $"reps.rse_id")
          )
          .select(
            "with_prefix.rse_id",
            "with_preifx.scope",
            "with_prefix.name",
            "reps.path"
          )
          .withColumn("path", when($"path".isNull, get_path($"prefix", $"scope", $"name")))

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
          .groupBy("rse", "scope", "name", "path").count()
          .orderBy(asc("rse"), asc("scope"), asc("name"))
          .select("rse,", "scope", "name", "path")

        val output_path = "/user/rucio01/tmp/" + date + "/suspicious_files"
        output
          .write
          .mode("overwrite")
          .option("delimiter", "\t")
          .csv(output_path)

        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))

        spark.stop()
    }
}