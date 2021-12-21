addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.6.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.1")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.6")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.40")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.8.1")
