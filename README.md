# SparkCodeSubmissionPlugin
Submit code to a running spark session

## Limitations

Needs jdk11+

For java virtual thread support (jdk19+) use --enable-preview jvm configuration (JAVA_TOOL_OPTIONS="--enable-preview" or spark.driver.extraJavaOptions=”--eanble-preview”) and maven repository https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo
else use https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/java11/repo

## Usage

#### As arguments to pyspark or spark-shell

```
pyspark --packages com.netapp.spark:codesubmit:1.0.0 --repositories https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo --conf spark.plugins com.netapp.spark.SparkCodeSubmissionPlugin --conf spark.code.submission.port=9001
```

#### As spark configuration

```
spark.jars.packages=com.netapp.spark:codesubmit:1.0.0
spark.jars.repositories=https://raw.githubusercontent.com/sigmarkarl/SparkCodeSubmissionPlugin/main/repo
spark.plugins=com.netapp.spark.SparkCodeSubmissionPlugin
spark.code.submission.port=9001
```

##### As a server (on a sidecar)

Use image public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:1.0.0

## Using with Spark Connect

Include the Spark Connect plugin in your spark application

```
spark.jars.packages=org.apache.spark:spark-connect:3.4.0
spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

Use the SPARK_REMOTE environment variable to specify the Spark Connect server address in the Spark Code Submission server (or plugin)

```
SPARK_REMOTE=sc://spark-master:15002
```

## Submit JSON format
Create a POST requeest on the server port

```
{
  "type": "SQL",
  "code": "select random()",
  "resultFormat": "csv",
  "resultPath": "s3://mybucket/sql_results.csv"
  "environment": {}
}
```

For Python code

```
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
spark.sql("select random()").write.format("csv").mode("overwrite").save(sys.argv[0])
```

if needed change to base64 encoding

```
{
  "type": "Python_base64",
  "code": "ZnJvbSBweXNwYXJrLnNxbCBpbXBvcnQgU3BhcmtTZXNzaW9uCnNwYXJrID0gU3BhcmtTZXNzaW9uLmJ1aWxkZXIubWFzdGVyKCJsb2NhbCIpLmFwcE5hbWUoInRlc3QiKS5nZXRPckNyZWF0ZSgpCnNwYXJrLnNxbCgic2VsZWN0IHJhbmRvbSgpIikud3JpdGUuZm9ybWF0KCJjc3YiKS5tb2RlKCJvdmVyd3JpdGUiKS5zYXZlKCJ0ZXN0LmNzdiIp",
  "arguments": "test.csv"
}
```

Allowed code values: SQL, R, Python, Java and Kotlin
