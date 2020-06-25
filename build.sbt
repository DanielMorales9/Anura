import sbt.Keys.scalaVersion

lazy val commonDependencies = Seq(
  "com.twitter" 			%% "scrooge-core"    % "latest.release",
  "com.twitter"       %% "finagle-core"  % "latest.release",
  "com.twitter"       %% "finagle-thrift"  % "latest.release"
)

lazy val serverDependencies = commonDependencies ++ Seq(
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release"
)

lazy val root = (project in file("."))
  .aggregate(server, commons)

lazy val commonSettings = Seq(
  target := { baseDirectory.value / "target" },
  scalaVersion := "2.13.2",
  version := "0.0.1-SNAPSHOT"
)

//lazy val client = (project in file("client"))
//  .settings(commonSettings)
//  .dependsOn(protocol)

lazy val commons = (project in file("commons"))
  .settings(
    commonSettings,
    name := "anura-commons",
    libraryDependencies := commonDependencies
  )

lazy val server = (project in file("server"))
  .settings(
    commonSettings,
    name := "anura",
    libraryDependencies := serverDependencies)
  .dependsOn(commons)

