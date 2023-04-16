# SparkCodeSubmissionPlugin
Submit code to a running spark session

## Limitations

Needs jdk11+

For java virtual thread support (jdk19+) use --enable-preview jvm configuration (JAVA_TOOL_OPTIONS="--enable-preview" or spark.driver.extraJavaOptions=”--eanble-preview”) and maven repository https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo
else use https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/java11/repo

## Usage

pyspark --packages com.netapp.spark:codesubmit:1.0.0 --repositories https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo --conf spark.plugins com.netapp.spark.SparkCodeSubmissionPlugin --conf spark.code.submission.port=9001

or as spark configuration

spark.jars.packages=com.netapp.spark:codesubmit:1.0.0
spark.jars.repositories=https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo
spark.plugins=com.netapp.spark.SparkCodeSubmissionPlugin
spark.code.submission.port=9001

## Submit JSON format

```
{
  "type": "SQL",
  "code": "select random()"
}
```
