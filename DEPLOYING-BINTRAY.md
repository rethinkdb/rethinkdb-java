## Deploying a release or snapshot to Bintray

To deploy, you'll need to create a file called `confidential.properties` in the same directory as `build.gradle.kts` 
(Alternatively, you can do the same`gradle.properties` at `~/.gradle` (`%USERPROFILE%\.gradle` on Windows) with the following:

```
bintray.user=<BINTRAY_USERNAME>
bintray.key=<BINTRAY_API_KEY>

ossrhUsername=<SONATYPE_USERNAME>
ossrhPassword=<SONATYPE_PASSWORD>

signing.password=
```

You should note that there's a `gradle.properties` in this repository, but you shouldn't add the above into it,
otherwise your credentials can be checked back into git. Create the `confidential.properties` file, which is added into
`.gitignore`, or create the file in the `.gradle` folder, in order to prevent accidents.

When releasing through Bintray, gpg signing is done by Bintray servers, so public and private-key must be configured on
their website. If you don't have a password on your private key for package signing, leave the `signing.password=` line **empty**.

To upload a new release, run the Gradle task `bintrayUpload`. This should upload the package to the bintray repository.
the version looks like `2.2` or `2.2-SNAPSHOT`, so this is important to get right or it won't go to the right place.

## Maven Central Integration

If you are doing a full release, you also need a Sonatype username and password, that you may get from [Sonatype's JIRA](https://issues.sonatype.org/secure/Signup!default.jspa),
with access to the `com.rethinkdb` group. Some RethinkDB maintainer may already have it.

After release, you may need to go to https://oss.sonatype.org/#stagingRepositories and search for "rethinkdb" in the
search box, find the release that is in status `open`. Select it and then click the `Close` button.
This will check it and make it ready for release. If that stage passes you can click the `Release` button.

For full instructions see: http://central.sonatype.org/pages/releasing-the-deployment.html
