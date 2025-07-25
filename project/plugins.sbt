resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")
addSbtPlugin("com.github.sbt" % "sbt-java-formatter" % "0.10.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.9.3")

// remember to bump in Dependencies.scala and samples/grpc/ projects as well if changing
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.5.5")

// Documentation
addSbtPlugin("io.akka" % "sbt-paradox-akka" % "25.10.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.4")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.4")
