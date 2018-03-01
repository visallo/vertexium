1. `mvn release:clean release:prepare -Dtag=vertexium-root-3.2.0 -DreleaseVersion=3.2.0 -DdevelopmentVersion=3.2.1-SNAPSHOT`
1. Change `multimodule-test` poms to next version
1. `mvn release:perform`
1. Go to https://oss.sonatype.org/#welcome and click "Staging Repositories" on the left
1. Find `orgvertexium...` and click "Close"
1. After it is closed, click "Release"
