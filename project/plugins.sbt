addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

// remember to bump in samples/grpc/ projects as well if changing
// FIXME remove dependency to ssl-config-core when updating to AKka grpc/HTTP version without dependency to ssl-config-core
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.3.4")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.51")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
