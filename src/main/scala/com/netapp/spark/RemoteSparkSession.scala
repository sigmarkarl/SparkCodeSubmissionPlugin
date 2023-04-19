package com.netapp.spark

import org.apache.spark.sql.SparkSession

object RemoteSparkSession {
  def create(remotepath: String): SparkSession = {
    SparkSession.builder()
      .appName("SparkCodeSubmissionServer")
      //.remote(remotepath)
      .getOrCreate()
  }
}