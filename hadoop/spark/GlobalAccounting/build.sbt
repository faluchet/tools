name := "GlobalAccounting"
 
version := "1.0" 
 
scalaVersion := "2.12.15" 

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" 
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.3.1" 
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-30" % "7.17.8"
