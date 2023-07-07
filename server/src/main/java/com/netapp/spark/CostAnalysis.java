package com.netapp.spark;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

public class CostAnalysis extends SparkListener {
    public CostAnalysis() {

    }

    public void onApplicationStart() {
    }

    public void onApplicationEnd() {
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {

    }

    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {

    }
}
