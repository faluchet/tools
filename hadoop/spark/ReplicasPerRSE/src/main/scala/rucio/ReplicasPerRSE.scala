package rucio

import java.security.MessageDigest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.functions.{col, udf, asc, lit, coalesce}
import org.apache.spark.sql.types.TimestampType
import org.apache.hadoop.fs.{FileSystem,Path}

object ReplicasPerRSE { 
    def main(args: Array[String]) {
        if (args.length != 1) {
            println("date has to be specified")
        }

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"
        val date = args(0)

        val get_path = udf((scope:String, name:String) => {
            val hstr = MessageDigest.getInstance("MD5").digest(("%s:%s".format(scope, name)).getBytes)
            var fixed_scope = scope
            if (scope.startsWith("user") || scope.startsWith("group")) {
                fixed_scope = scope.replace(".", "/")
            }
            "/%s/%02x/%02x/%s".format(fixed_scope, hstr(0), hstr(1), name)
        })

        val spark = SparkSession.builder.appName("Rucio Replicas per RSE").getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.shuffle.partitions", "300")
        import spark.implicits._

        val replicas = spark
          .read
          .format("avro")
          .load("%s/%s/%s/replicas".format(base_dir, dumps_dir, date))
        
        val rses = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))

        val join_replicas_rses = replicas.as("replicas")
          .join(
            rses.as("rses"),
            $"replicas.RSE_ID" === $"rses.ID"
          )
          .select(
            $"rses.rse".as("rse"),
            $"rses.rse".as("_rse"),
            $"replicas.scope".as("scope"),
            $"replicas.name".as("name"),
            $"replicas.adler32".as("adler32"),
            $"replicas.bytes".as("bytes"),
            $"replicas.created_at".divide(1000).cast(TimestampType).as("created_at"),
            $"replicas.path".as("path"),
            $"replicas.updated_at".divide(1000).cast(TimestampType).as("updated_at"),
            $"replicas.state".as("state"),
            $"replicas.accessed_at".divide(1000).cast(TimestampType).as("accessed_at"),
            $"replicas.tombstone".divide(1000).cast(TimestampType).as("tombstone")
          )

        val filter_det = join_replicas_rses.filter("path is null")
        val filter_nondet = join_replicas_rses.filter("path is not null")

        val add_path = filter_det.withColumn("path", get_path($"scope", $"name"))

        val union_all = add_path.union(filter_nondet)

        val count_replicas = replicas
          .filter($"state" === "A")
          .select(
            "scope",
            "name"
          )
          .groupBy("scope", "name")
          .count()

        val add_replicas_count = union_all.as("reps")
          .join(
            count_replicas.as("count"),
            $"reps.scope" === $"count.scope" &&
            $"reps.name" === $"count.name",
            "leftouter"
          )
          .select(
            $"reps.rse",
            $"reps._rse",
            $"reps.scope",
            $"reps.name",
            $"reps.adler32",
            $"reps.bytes",
            $"reps.created_at",
            $"reps.path",
            $"reps.updated_at",
            $"reps.state",
            $"reps.accessed_at",
            $"reps.tombstone",
            coalesce($"count.count", lit(0))
          )

        val ordered = add_replicas_count.orderBy(asc("path"))

        val output_path = "%s/%s/%s/replicas_per_rse".format(base_dir, reports_dir, date)
        ordered
          .repartition($"rse")
          .write
          .partitionBy("rse")
          .option("delimiter", "\t")
          .option("compression","bzip2")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv(output_path)

        // Rename folder to get rid of "rse="
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))
        spark.stop()
    }
}
