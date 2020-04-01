package rucio

import java.security.MessageDigest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.functions.{col, udf, asc}
import org.apache.spark.sql.types.TimestampType
import org.apache.hadoop.fs.{FileSystem,Path}

object ReplicasPerRSE { 
    def main(args: Array[String]) {
        if (args.length != 1) {
            println("date has to be specified")
        }

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
        spark.conf.set("spark.executor.memory", "8G")
        spark.conf.set("spark.sql.shuffle.partitions", "100")
        spark.conf.set("spark.dynamicAllocation.maxExecutors", "200")
        import spark.implicits._

        val replicas = spark.read.format("avro").load("/user/rucio01/dumps/" + date + "/replicas")
        
        val rses = spark.read.format("avro").load("/user/rucio01/dumps/" + date + "/rses")

        val join_replicas_rses = replicas.as("replicas")
          .join(
            rses.as("rses"),
            col("replicas.RSE_ID") === col("rses.ID")
          )
          .select(
            col("rses.rse"),
            col("rses.rse").as("_rse"),
            col("replicas.scope"),
            col("replicas.name"),
            col("replicas.adler32"),
            col("replicas.bytes"),
            col("replicas.created_at").divide(1000).cast(TimestampType),
            col("replicas.path"),
            col("replicas.updated_at").divide(1000).cast(TimestampType),
            col("replicas.state"),
            col("replicas.accessed_at").divide(1000).cast(TimestampType),
            col("replicas.tombstone").divide(1000).cast(TimestampType)
          )

        val filter_det = join_replicas_rses.filter("path is null")
        val filter_nondet = join_replicas_rses.filter("path is not null")

        val add_path = filter_det.withColumn("path", get_path(col("replicas.scope"), col("replicas.name")))

        val union_all = add_path.union(filter_nondet)
        val ordered = union_all.orderBy(asc("path"))

        val output_path = "/user/rucio01/tmp/" + date + "/replicas_per_rse"
        ordered
          .repartition($"rse")
          .write
          .partitionBy("rse")
          .mode("overwrite")
          .option("delimiter", "\t")
          .option("compression","bzip2")
          .option("quote", "\u0000")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv(output_path)

        // Rename folder to get rid of "rse="
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val stageDirs = fs.listStatus(new Path(output_path)).map( _.getPath.toString)
        stageDirs.foreach( dir => fs.rename(new Path(dir),new Path(dir.replaceAll("rse=",""))))
        spark.stop()
    }
}