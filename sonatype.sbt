sonatypeProfileName := "com.twosigma"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("twosigma", "flint", "ljin@twosigma.com"))

homepage := Some(url("https://github.com/twosigma/flint"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/twosigma/flint"),
    "scm:git@github.com:twosigma/flint.git"
  )
)

developers := List(
  Developer(
    id    = "icexelloss",
    name  = "Li Jin",
    email = "ice.xelloss@gmail.com",
    url   = url("https://github.com/icexelloss")
  )
)

publishArtifact in Test := false


