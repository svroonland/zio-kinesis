resolvers += "Artifactory" at "https://boomtrain.jfrog.io/artifactory/sbt"

addSbtPlugin("com.liveintent" % "li-sbt-plugins" % "8.21.0")
addSbtPlugin("com.github.sbt" % "sbt-protobuf"   % "0.8.3")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"   % "2.5.6")

libraryDependencies += "com.sun.activation" % "javax.activation" % "1.2.0"

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
