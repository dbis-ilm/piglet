logLevel := Level.Warn

// resolvers += Resolver.url(
//   "bintray-sbt-plugin-releases",
//   url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
//     Resolver.ivyStylePatterns)
//
// addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
