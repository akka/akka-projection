resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.4.0-M1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.13")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
