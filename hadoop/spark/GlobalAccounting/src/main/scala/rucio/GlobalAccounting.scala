package rucio

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, udf, asc, max, min, collect_list, concat_ws, lit, coalesce, when, sum, count}
import org.apache.spark.sql.types.{TimestampType, LongType}
import org.apache.hadoop.fs.{FileSystem,Path}

object GlobalAccounting { 
    def main(args: Array[String]) {
        if (args.length != 1) {
            println("date has to be specified")
            System.exit(1)
        }

        val base_dir = "/user/rucio01"
        val dumps_dir = "dumps"
        val reports_dir = "reports"
        val date = args(0)
        
        val spark = SparkSession.builder.appName("Rucio Global Accounting").getOrCreate()

        spark.conf.set("spark.sql.session.timeZone", "UTC")
        import spark.implicits._

        val rses = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rses".format(base_dir, dumps_dir, date))
        
        val rse_metadata = spark
          .read
          .format("avro")
          .load("%s/%s/%s/rse_metadata".format(base_dir, dumps_dir, date))
        
        val get_rse_meta = rses.as("rses")
          .join(
            rse_metadata.as("meta"),
            $"rses.id" === $"meta.rse_id"
          )
          .select(
            $"rses.id",
            $"rses.rse",
            $"rses.rse_type",
            $"meta.key",
            $"meta.value"
          )
        
        val get_type = get_rse_meta
          .filter($"key" === "type")
          .select(
            $"id",
            $"rse",
            $"rse_type",
            $"value".as("type"),
            lit(-2).as("tier")
          )

        val get_tier = get_rse_meta
          .filter($"key" === "tier")
          .select(
            $"id",
            $"rse",
            $"rse_type",
            lit(-2).as("type"),
            $"value".as("tier")
          )
        val get_rses = get_type
          .union(get_tier)
          .groupBy("id", "rse", "rse_type")
          .agg(
            max($"type").as("type"),
            max($"tier").as("tier")
          )
          .filter("(type = \"DATADISK\" AND tier != \"3\") OR rse = \"CERN-PROD_TZDISK\" OR rse_type = \"TAPE\"")
          .select(
            "id",
            "rse",
            "rse_type",
            "tier"
          )

        val replicas = spark
          .read
          .format("avro")
          .load("%s/%s/%s/replicas".format(base_dir, dumps_dir, date))
          .select(
            $"scope",
            $"name",
            $"rse_id",
            coalesce($"bytes", lit(0)).as("bytes"),
            $"tombstone"
          ).
          withColumn("type", when($"tombstone".isNull, lit("primary")).otherwise(lit("secondary")))
        
        val get_reps = replicas.as("reps")
          .join( 
            get_rses.as("rses"),
            $"reps.RSE_ID" === $"rses.ID" )
          .select(
            $"reps.scope",
            $"reps.name",
            $"reps.bytes".cast(LongType),
            $"rses.rse",
            $"reps.type",
            $"rses.rse_type",
            $"rses.tier"
          )

        val contents = spark
          .read
          .format("avro")
          .load("%s/%s/%s/contents".format(base_dir, dumps_dir, date))
          .filter("did_type = \"D\" AND child_type = \"F\"")
          .select(
            $"scope",
            $"name",
            $"child_scope".as("cscope"),
            $"child_name".as("cname")
          )

        val get_datasets = get_reps.as("reps")
          .join(
            contents.as("cont"),
            col("reps.scope") === col("cont.cscope")
            && col("reps.name") === col("cont.cname"),
            "leftouter"
          )
          .select(
            coalesce($"cont.scope", lit("no_scope")).as("dscope"),
            coalesce($"cont.name", lit("no_name")).as("dname"),
            $"reps.scope".as("fscope"),
            $"reps.name".as("fname"),
            $"reps.rse",
            $"reps.type",
            $"reps.bytes",
            $"reps.rse_type",
            $"reps.tier"
          )
          .filter("dscope != \"panda\"")
          .groupBy("fscope", "fname", "rse")

        val select_dataset = udf((scopes:Seq[String], names:Seq[String]) => {
            if (scopes.length == 1) {
              (scopes(0), names(0))
            } else {
              var i = 0
              var min = "%s:%s".format(scopes(0), names(0))
              if (scopes(0) == "panda") {
                i = 1
                min = scopes(1)
              }
              for ( j <- i to (scopes.length - 1)) {
                if (scopes(j) != "panda") {
                  val tmp = "%s:%s".format(scopes(j), names(j))
                  if (tmp < min) {
                    i = j
                    min = tmp
                  }
                }
              }
              (scopes(i), names(i))
            }
        })

        val one_ds = get_datasets
          .agg(
            max("bytes").as("bytes"),
            max("type").as("type"),
            max("rse_type").as("rse_type"),
            max("tier").as("tier"),
            collect_list($"dscope").as("dscopes"),
            collect_list($"dname").as("dnames")
          )
          .withColumn("datasets", select_dataset($"dscopes", $"dnames"))
          .drop("dscopes")
          .drop("dnames")
          .select( 
            "fscope",
            "fname",
            "datasets.*",
            "bytes",
            "type",
            "rse_type",
            "tier"
          )
          .withColumnRenamed("_1","dscope")
          .withColumnRenamed("_2","dname")

        val prims_t0 = one_ds
          .filter("type = \"primary\" AND rse_type = \"DISK\" and tier = \"0\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("prim_t0_files"),
            sum("bytes").as("prim_t0_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            $"prim_t0_files",
            $"prim_t0_bytes",
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val prims_t1 = one_ds
          .filter("type = \"primary\" AND rse_type = \"DISK\" and tier = \"1\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("prim_t1_files"),
            sum("bytes").as("prim_t1_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            $"prim_t1_files",
            $"prim_t1_bytes",
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val prims_t2 = one_ds
          .filter("type = \"primary\" AND rse_type = \"DISK\" and tier = \"2\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("prim_t2_files"),
            sum("bytes").as("prim_t2_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            $"prim_t2_files",
            $"prim_t2_bytes",
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val secs_t0 = one_ds
          .filter("type = \"secondary\" AND rse_type = \"DISK\" and tier = \"0\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("secs_t0_files"),
            sum("bytes").as("secs_t0_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            $"secs_t0_files",
            $"secs_t0_bytes",
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val secs_t1 = one_ds
          .filter("type = \"secondary\" AND rse_type = \"DISK\" and tier = \"1\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("secs_t1_files"),
            sum("bytes").as("secs_t1_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            $"secs_t1_files",
            $"secs_t1_bytes",
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val secs_t2 = one_ds
          .filter("type = \"secondary\" AND rse_type = \"DISK\" and tier = \"2\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("secs_t2_files"),
            sum("bytes").as("secs_t2_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            $"secs_t2_files",
            $"secs_t2_bytes",
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val tape_t0 = one_ds
          .filter("rse_type = \"TAPE\" and tier = \"0\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("tape_t0_files"),
            sum("bytes").as("tape_t0_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            $"tape_t0_files",
            $"tape_t0_bytes",
            lit(0).as("tape_t1_files"),
            lit(0).as("tape_t1_bytes")
          )
        val tape_t1 = one_ds
          .filter("rse_type = \"TAPE\" and tier = \"1\"")
          .groupBy("dscope", "dname")
          .agg(
            count("fname").as("tape_t1_files"),
            sum("bytes").as("tape_t1_bytes")
          )
          .select(
            $"dscope",
            $"dname",
            lit(0).as("prim_t0_files"),
            lit(0).as("prim_t0_bytes"),
            lit(0).as("prim_t1_files"),
            lit(0).as("prim_t1_bytes"),
            lit(0).as("prim_t2_files"),
            lit(0).as("prim_t2_bytes"),
            lit(0).as("secs_t0_files"),
            lit(0).as("secs_t0_bytes"),
            lit(0).as("secs_t1_files"),
            lit(0).as("secs_t1_bytes"),
            lit(0).as("secs_t2_files"),
            lit(0).as("secs_t2_bytes"),
            lit(0).as("tape_t0_files"),
            lit(0).as("tape_t0_bytes"),
            $"tape_t1_files",
            $"tape_t1_bytes"
          )

        val union_all = prims_t0.union(prims_t1).union(prims_t2).union(secs_t0).union(secs_t1).union(secs_t2).union(tape_t0).union(tape_t1)
          .groupBy("dscope", "dname")
          .agg(
            max("prim_t0_files").as("prim_t0_files"),
            max("prim_t0_bytes").as("prim_t0_bytes"),
            max("prim_t1_files").as("prim_t1_files"),
            max("prim_t1_bytes").as("prim_t1_bytes"),
            max("prim_t2_files").as("prim_t2_files"),
            max("prim_t2_bytes").as("prim_t2_bytes"),
            max("secs_t0_files").as("secs_t0_files"),
            max("secs_t0_bytes").as("secs_t0_bytes"),
            max("secs_t1_files").as("secs_t1_files"),
            max("secs_t1_bytes").as("secs_t1_bytes"),
            max("secs_t2_files").as("secs_t2_files"),
            max("secs_t2_bytes").as("secs_t2_bytes"),
            max("tape_t0_files").as("tape_t0_files"),
            max("tape_t0_bytes").as("tape_t0_bytes"),
            max("tape_t1_files").as("tape_t1_files"),
            max("tape_t1_bytes").as("tape_t1_bytes")
          )
        
        val dids = spark
          .read
          .format("avro")
          .load("%s/%s/%s/dids".format(base_dir, dumps_dir, date))
          .select(
            $"scope",
            $"name",
            coalesce($"length", lit(0)).as("length").cast(LongType),
            coalesce($"bytes", lit(0)).as("bytes").cast(LongType),
            $"datatype",
            $"prod_step",
            $"run_number",
            $"project",
            $"stream_name",
            $"version",
            $"campaign",
            coalesce($"events", lit(0)).as("events").cast(LongType)
          )

        val get_repl_factor = union_all.as("reps")
          .join(
            dids.as("dids"),
            (($"reps.dscope" === $"dids.scope") &&
            ($"reps.dname" === $"dids.name")),
            "leftouter"
          )
          .select(
            $"reps.dscope".as("scope"),
            $"reps.dname".as("name"),
            coalesce($"dids.length", lit(0)),
            coalesce($"dids.bytes", lit(0)),
            $"reps.prim_t0_files",
            $"reps.prim_t1_files",
            $"reps.prim_t2_files",
            $"reps.secs_t0_files",
            $"reps.secs_t1_files",
            $"reps.secs_t2_files",
            $"reps.tape_t0_files",
            $"reps.tape_t1_files",
            $"reps.prim_t0_bytes",
            $"reps.prim_t1_bytes",
            $"reps.prim_t2_bytes",
            $"reps.secs_t0_bytes",
            $"reps.secs_t1_bytes",
            $"reps.secs_t2_bytes",
            $"reps.tape_t0_bytes",
            $"reps.tape_t1_bytes",
            ($"reps.prim_t0_files" / $"dids.length").as("prim_repl_factor_t0"),
            ($"reps.prim_t1_files" / $"dids.length").as("prim_repl_factor_t1"),
            ($"reps.prim_t2_files" / $"dids.length").as("prim_repl_factor_t2"),
            ($"reps.secs_t0_files" / $"dids.length").as("secs_repl_factor_t0"),
            ($"reps.secs_t1_files" / $"dids.length").as("secs_repl_factor_t1"),
            ($"reps.secs_t2_files" / $"dids.length").as("secs_repl_factor_t2"),
            ($"reps.tape_t0_files" / $"dids.length").as("tape_repl_factor_t0"),
            ($"reps.tape_t1_files" / $"dids.length").as("tape_repl_factor_t1"),
            $"dids.datatype",
            $"dids.prod_step",
            $"dids.run_number",
            $"dids.project",
            $"dids.stream_name",
            $"dids.version",
            $"dids.campaign",
            coalesce($"dids.events", lit(0))
          )
          .orderBy(asc("scope"), asc("name"))

        val output_path = "%s/%s/%s/global_accounting".format(base_dir, reports_dir, date)
        get_repl_factor
            .write
            .option("delimiter", "\t")
            .csv(output_path)

        spark.stop()
    }
}