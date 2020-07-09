/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

licenses += ("Apache-2.0", url("http://apache.org/licenses/LICENSE-2.0"))

lazy val formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences().
    setPreference(AlignParameters, false).
    setPreference(PreserveSpaceBeforeArguments, true).
    setPreference(SpacesAroundMultiImports, true)
}

val _scalaVersion: String = sys.props.getOrElse("scala.version", default = "2.12.8")
val _sparkVersion: String = sys.props.getOrElse("spark.version", default = "2.4.3")

lazy val compilationSettings = scalariformSettings ++ Seq(
  name := "sparklyr-flint_" +
    _sparkVersion.substring(0, _sparkVersion.lastIndexOf(".")) + "_" +
    _scalaVersion.substring(0, _scalaVersion.lastIndexOf(".")),
  version := sys.props.getOrElse("version", default = "0.6.0-SNAPSHOT"),
  organization := "org.sparklyr",
  scalaVersion := _scalaVersion,
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
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
  ),
  ScalariformKeys.preferences := formattingPreferences
)

lazy val versions = new {
  val play_json = "2.7.4"
  val commons_math = "3.5"
  val joda_time = "2.9.4"
  val httpclient = "4.3.2" // Note that newer versions need to be configured
  val spark = _sparkVersion
  val scalatest = "3.0.8"
  val scalacheck = "1.12.6"
  val grizzled_slf4j = "1.3.0"
  val arrow = "0.12.0"
  val jackson_module = "2.7.9"
}

lazy val lazyDependencies = new {
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark % "provided"
  val sparkML = "org.apache.spark" %% "spark-mllib" % versions.spark % "provided"
  val sparkSQL = "org.apache.spark" %% "spark-sql" % versions.spark % "provided"
}

lazy val dependencySettings = libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % versions.play_json % "test",
  "org.apache.commons" % "commons-math3" % versions.commons_math,
  "joda-time" % "joda-time" % versions.joda_time,
  "org.apache.httpcomponents" % "httpclient" % versions.httpclient,
  "org.clapper" %% "grizzled-slf4j" % versions.grizzled_slf4j,
  lazyDependencies.sparkCore,
  lazyDependencies.sparkML,
  lazyDependencies.sparkSQL,
  "org.scalatest" %% "scalatest" % versions.scalatest % "test",
  "org.scalacheck" %% "scalacheck" % versions.scalacheck % "test",
  // These jackson modules are not directly used by Flint. We need to put it
  // there because Spark 2.3 uses Jackson 2.6 and Arrow uses Jackson
  // 2.7. We should be able to remove these once we moved to newer Spark
  // version.
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % versions.jackson_module,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % versions.jackson_module
)

lazy val flint = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(compilationSettings)
  .settings(dependencySettings)
  .settings(parallelExecution in Test := false)
  .settings(testOptions in Test += Tests.Argument("-oDF"))
  .settings(apiMappings ++= DocumentationMapping.mapJarToDocURL(
    (managedClasspath in (Compile, doc)).value,
    Seq(
      DocumentationMapping(url(s"http://spark.apache.org/docs/${versions.spark}/api/scala/"),
        lazyDependencies.sparkCore, lazyDependencies.sparkML, lazyDependencies.sparkSQL
      )
    )
  ))

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.startsWith("META-INF/services") => MergeStrategy.filterDistinctLines
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case m if m.startsWith("git.properties") => MergeStrategy.discard
    case _ => MergeStrategy.deduplicate
  }
}

addCommandAlias(
  "assemblyNoTest",
  "; set test in Test := {}; assembly"
)

crossPaths := false
