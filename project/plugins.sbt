resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

// remember to bump in samples/grpc/ projects as well if changing
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.4.1")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.55")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.4")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
