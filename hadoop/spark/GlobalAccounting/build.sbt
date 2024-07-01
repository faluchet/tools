name := "GlobalAccounting"
 
version := "1.3"
 
scalaVersion := "2.12.17"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.4.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-30" % "7.17.8"
libraryDependencies += "org.opensearch.client" %% "opensearch-spark-30" % "1.2.0"
