/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import de.heikoseeberger.sbtheader.HeaderPattern
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import BuildUtil._

lazy val tsOpenSourceHeader = (
  HeaderPattern.cStyleBlockComment,
  """|/*
     | *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
     | *
     | *  Licensed under the Apache License, Version 2.0 (the "License");
     | *  you may not use this file except in compliance with the License.
     | *  You may obtain a copy of the License at
     | *
     | *    http://www.apache.org/licenses/LICENSE-2.0
     | *
     | *  Unless required by applicable law or agreed to in writing, software
     | *  distributed under the License is distributed on an "AS IS" BASIS,
     | *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     | *  See the License for the specific language governing permissions and
     | *  limitations under the License.
     | */
     |
     |""".stripMargin
)

lazy val formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences().
    setPreference(AlignParameters, false).
    setPreference(PreserveSpaceBeforeArguments, true).
    setPreference(SpacesAroundMultiImports, true)
}

lazy val compilationSettings = scalariformSettings ++ Seq(
  version := "0.2.0-SNAPSHOT",
  organization := "com.twosigma",
  scalaVersion := "2.11.8",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
  compileOrder in Compile := CompileOrder.JavaThenScala,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen"
  ),
  resolvers ++= Seq(
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
  ),
  headers := Map(
    "scala" -> tsOpenSourceHeader,
    "java" -> tsOpenSourceHeader
  ),
  ScalariformKeys.preferences := formattingPreferences
)

lazy val versions = new {
  val play_json = "2.3.10"
  val commons_math = "3.5"
  val joda_time = "2.9.4"
  val httpclient = "4.3.2" // Note that newer versions need to be configured differently
  val spark = sys.props.getOrElse("spark.version", default = "2.1.0")
  val scalatest = "2.2.4"
  val grizzled_slf4j = "1.3.0"
}

lazy val lazyDependencies = new {
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark % "provided"
  val sparkML = "org.apache.spark" %% "spark-mllib" % versions.spark % "provided"
  val sparkSQL = "org.apache.spark" %% "spark-sql" % versions.spark % "provided"
}

lazy val dependencySettings = libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % versions.play_json,
  "org.apache.commons" % "commons-math3" % versions.commons_math,
  "joda-time" % "joda-time" % versions.joda_time,
  "org.apache.httpcomponents" % "httpclient" % versions.httpclient,
  "org.clapper" %% "grizzled-slf4j" % versions.grizzled_slf4j,
  lazyDependencies.sparkCore,
  lazyDependencies.sparkML,
  lazyDependencies.sparkSQL,
  "org.scalatest" %% "scalatest" % versions.scalatest % "test"
)

lazy val flint = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(compilationSettings)
  .settings(dependencySettings)
  .settings(parallelExecution in Test := false)
  .settings(apiMappings ++= DocumentationMapping.mapJarToDocURL(
    (managedClasspath in (Compile, doc)).value,
    Seq(
      DocumentationMapping(url(s"http://spark.apache.org/docs/${versions.spark}/api/scala/"),
        lazyDependencies.sparkCore, lazyDependencies.sparkML, lazyDependencies.sparkSQL
      )
    )
  ))
  .settings(
    headers := Map(
      "scala" -> tsOpenSourceHeader,
      "java" -> tsOpenSourceHeader
    )
  )
