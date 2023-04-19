build:
	./gradlew build

publish:
	./gradlew publishToMavenLocal

ivy:
	./gradlew publishIvyPublicationToLocalRepository

clean:
	./gradlew clean

maven:
	mvn install:install-file -DgroupId=com.netapp.spark -DartifactId=codesubmit -Dversion=1.0.0 -Dfile=build/libs/SparkCodeSubmissionPlugin-1.0.0.jar -Dpackaging=jar -DlocalRepositoryPath=. -DcreateChecksum=true -DgeneratePom=true