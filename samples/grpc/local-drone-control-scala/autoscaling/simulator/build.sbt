scalaVersion := "2.13.16"

enablePlugins(GatlingPlugin)

libraryDependencies ++= Seq(
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "3.9.5" % Test,
  "io.gatling" % "gatling-test-framework" % "3.9.5" % Test,
  "com.github.phisgr" % "gatling-grpc" % "0.16.0" % Test,
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf")

Test / PB.targets := Seq(scalapb.gen() -> (Test / sourceManaged).value / "scalapb")
