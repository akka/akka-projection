addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.7.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.7.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.42")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.8.1")
