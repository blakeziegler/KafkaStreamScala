val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "iotrt",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.8.0",
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.8.0",
    libraryDependencies += "org.apache.kafka" % "kafka-streams-scala_2.13" % "3.8.0",
    // https://mvnrepository.com/artifact/io.circe/circe-core
    libraryDependencies += "io.circe" %% "circe-core" % "0.15.0-M1",
    libraryDependencies += "io.circe" %% "circe-generic" % "0.15.0-M1",
    libraryDependencies += "io.circe" %% "circe-parser" % "0.15.0-M1"
    

  )

