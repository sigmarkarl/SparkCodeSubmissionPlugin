package com.netapp.spark;

import java.util.List;
import java.util.Map;

public record CodeSubmission(CodeSubmissionType type, String code, String className, List<String> arguments, Map<String,String> environment, String config, String resultFormat, String resultsPath) {}