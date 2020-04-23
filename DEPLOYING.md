## Deploying a release or snapshot to Bintray

To deploy, you'll need to create a file called `confidential.properties` in the same directory as `build.gradle.kts` 
(Alternatively, you can do the same`gradle.properties` at `~/.gradle` (`%USERPROFILE%\.gradle` on Windows) with the following:

```
bintray.user=<BINTRAY_USERNAME>
bintray.key=<BINTRAY_API_KEY>

ossrhUsername=<SONATYPE_USERNAME>
ossrhPassword=<SONATYPE_PASSWORD>

signing.keyId=<SIGNING_KEY_ID>
signing.password=<SIGNING_KEY_PASSWORD>
signing.secretKeyRingFile=<SIGNING_KEY_FILE>
```

You should note that there's a `gradle.properties` in this repository, but you shouldn't add the above into it, otherwise your credentials can be checked back into git. Create the `confidential.properties` file, which is added into `.gitignore`, or create the file in the `.gradle` folder, in order to prevent accidents.

When releasing through Bintray, gpg signing is done by the person who does the release. The reason for not letting Bintray sign the packages is simply security consideration. You'll need to add your gpg signing key id and keyring file. Usually, the keyring file is located at`~/.gnupg/secring.gpg`, but Gradle won't expand home-dirs in the config file so you have to put the absolute path to your keyring file. If you don't have the private key for package signing, ask one of the **@rethinkdb/team-java** members to help you deploy.

You must use gpg 2.0 or below, since gpg 2.1 and greater doesn't use the `secring.gpg` file anymore. This is a limitation on Gradle's end and there's an [issue regarding that](https://github.com/gradle/gradle/issues/888).

To upload a new release, run `./gradlew assemble signArchives bintrayUpload`. This should upload the package to the bintray repository. the version looks like `2.2` or `2.2-SNAPSHOT`, so this is important to get right or it won't go to the right place.

## Maven Central Integration

If you are doing a release, you also need a Sonatype username and password, that you may get from [Sonatype's JIRA](https://issues.sonatype.org/secure/Signup!default.jspa), with access to the `com.rethinkdb` group. Some RethinkDB maintainer may already have it.

To upload a new release directly to Sonatype, run the Gradle task `uploadArchives`. This should sign and upload the package to the release repository. This is for official releases/betas etc. If you just want to upload a snapshot, add the suffix `-SNAPSHOT` to the `version` value in `build.gradle`. The gradle maven plugin decides which repo to upload to depending on whether the version looks like `2.2` or `2.2-SNAPSHOT`, so this is important to get right or it won't go to the right place.

After release, you may need to go to https://oss.sonatype.org/#stagingRepositories and search for "rethinkdb" in the search box, find the release that is in status `open`. Select it and then click the `Close` button. This will check it and make it ready for release. If that stage passes you can click the `Release` button.

For full instructions see: http://central.sonatype.org/pages/releasing-the-deployment.html
