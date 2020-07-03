lazy val commonDependencies = Seq(
  "com.twitter" 			%% "scrooge-core"    % "latest.release",
  "com.twitter"       %% "finagle-core"  % "latest.release",
  "com.twitter"       %% "finagle-thrift"  % "latest.release",
  "com.storm-enroute" %% "scalameter" % "0.18"
)

lazy val serverDependencies = commonDependencies ++ Seq(
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release"
)

lazy val root = (project in file("."))
  .aggregate(server, commons)

lazy val commonSettings = Seq(
  target := { baseDirectory.value / "target" },
  scalaVersion := "2.13.2",
  version := "0.0.1-SNAPSHOT",
  libraryDependencies := commonDependencies
)

lazy val serverSettings = commonSettings ++ Seq(
  libraryDependencies := serverDependencies,
  resolvers := Seq("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"),
  testFrameworks := Seq(new TestFramework("org.scalameter.ScalaMeterFramework")),
  parallelExecution in Test := false,
  fork := true,
  outputStrategy := Some(StdoutOutput),
  connectInput := true
)

lazy val commons = (project in file("commons"))
  .settings(
    name := "anura-commons",
    commonSettings
  )

lazy val client = (project in file("client"))
  .settings(
    name := "anura-client",
    commonSettings
  )
  .dependsOn(commons)

lazy val server = (project in file("server"))
  .settings(
    name := "anura",
    serverSettings
  )
  .dependsOn(commons)


