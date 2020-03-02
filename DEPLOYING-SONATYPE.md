# Deploying a release or snapshot to Sonatype

To deploy, you'll need to create a file called `confidential.properties` in the same directory as `build.gradle.kts` 
(Alternatively, you can do the same`gradle.properties` at `~/.gradle` (`%USERPROFILE%\.gradle` on Windows) with the following:

```
ossrhUsername=<SONATYPE_USERNAME>
ossrhPassword=<SONATYPE_PASSWORD>

signing.keyId=<KEY_ID>
signing.password=
signing.secretKeyRingFile=<KEYRING_LOCATION>
```

You should note that there's a `gradle.properties` in this repository, but you shouldn't add the above into it,
otherwise your credentials can be checked back into git. Create the `confidential.properties` file, which is added into
`.gitignore`, or create the file in the `.gradle` folder, in order to prevent accidents.

You'll need to add your gpg signing key id and keyring file. Usually, the keyring file is located at`~/.gnupg/secring.gpg`,
but Gradle won't expand home-dirs in the config file so you have to put the absolute path to your keyring file.
If you don't have a password on your private key for package signing, leave the `signing.password=` line **empty**.

You must use gpg 2.0 or below, since gpg 2.1 and greater doesn't use the `secring.gpg` file anymore. This is a limitation
on Gradle's end and there's an [issue regarding that](https://github.com/gradle/gradle/issues/888).

You also neeed a Sonatype username and password, that you may get from [Sonatype's JIRA](https://issues.sonatype.org/secure/Signup!default.jspa),
with access to the `com.rethinkdb` group. Some RethinkDB maintainer may already have it.

To upload a new release, run the Gradle task `uploadArchives`. This should sign and upload the package to the release
repository. This is for official releases/betas etc. If you just want to upload a snapshot, add the suffix `-SNAPSHOT`
to the `version` value in `build.gradle`. The gradle maven plugin decides which repo to upload to depending on whether
the version looks like `2.2` or `2.2-SNAPSHOT`, so this is important to get right or it won't go to the right place.

If you just want to do a snapshot: if `gradle uploadArchives` succeeds, you're done. The snapshot will be located at
https://oss.sonatype.org/content/repositories/snapshots/com/rethinkdb/rethinkdb-driver/ with the version you gave it.

If you are doing a full release, you need to go to https://oss.sonatype.org/#stagingRepositories and search for
"rethinkdb" in the search box, find the release that is in status `open`. Select it and then click the `Close` button.
This will check it and make it ready for release. If that stage passes you can click the `Release` button.

For full instructions see: http://central.sonatype.org/pages/releasing-the-deployment.html
