name := "lab1"

organization := "se.kth.spark"

version := "1.0"

scalaVersion := "2.11.1"

//resolvers += Resolver.mavenLocal
resolvers += "Kompics Releases" at "http://kompics.sics.se/maven/repository/"
resolvers += "Kompics Snapshots" at "http://kompics.sics.se/maven/snapshotrepository/"

// Uncomment to run on Spark Cluster
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
//libraryDependencies += "org.log4s" %% "log4s" % "1.3.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "org.log4s" %% "log4s" % "1.3.3"

libraryDependencies += "se.kth.spark" %% "lab1_lib" % "1.0-SNAPSHOT"
libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"
libraryDependencies += "ch.systemsx.cisd" % "sis-base" % "1.0"
libraryDependencies += "ch.systemsx.cisd" % "sis-jhdf5-batteries_included" % "1.0"



mainClass in assembly := Some("se.kth.spark.lab1.task7.LinRegTextSongs")

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
