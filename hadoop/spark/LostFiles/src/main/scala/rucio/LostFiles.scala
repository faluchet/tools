package rucio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, collect_list, concat_ws, lit, coalesce}
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
            $"account",
            $"updated_at".divide(1000)
          )
          .filter($"created_at" >= start)
          .filter($"created_at" < end)
          .filter("state = 'L'")
        
        val contents_history = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/contents_history")
          .select(
            "scope",
            "name",
            "child_scope",
            "child_name",
            "did_type"
          )
          .filter("did_type = 'D'")
        
        val rses = spark
          .read
          .format("avro")
          .load("/user/rucio01/dumps/" + date + "/rses")
        
        val get_lost_files = bad_replicas.as("bad_reps")
          .join(contents_history.as("hist"), ($"bad_reps.scope" === $"hist.cscope") && ($"bad_reps.name" === $"hist.cname"))
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

        val output_path = "/user/rucio01/tmp/" + date + "/lost_files"
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