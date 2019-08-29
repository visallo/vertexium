1. `mvn release:clean release:prepare -Dtag=vertexium-root-4.7.2 -DreleaseVersion=4.7.2 -DdevelopmentVersion=4.7.3-SNAPSHOT`
1. Change `multimodule-test` poms to next version
1. `mvn release:perform`
1. Go to https://oss.sonatype.org/#welcome and click "Staging Repositories" on the left
1. Find `orgvertexium...` and click "Close"
1. After it is closed, click "Release"
1. Copy `CHANGELOG.md` items and update https://github.com/visallo/vertexium/releases
