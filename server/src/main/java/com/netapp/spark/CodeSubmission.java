package com.netapp.spark;

import java.util.List;
import java.util.Map;

public class CodeSubmission {
    CodeSubmissionType type;
    String code;
    String className;
    List<String> arguments;
    Map<String,String> environment;
    String config;
    String resultFormat;
    String resultsPath;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CodeSubmission) {
            var cs = (CodeSubmission)obj;
            return cs.type.equals(type) &&
                    cs.code.equals(code) &&
                    cs.className.equals(className) &&
                    cs.arguments.equals(arguments) &&
                    cs.config.equals(config) &&
                    cs.resultFormat.equals(resultFormat) &&
                    cs.resultsPath.equals(resultsPath);
        }
        return false;
    }

    public CodeSubmissionType getType() {
        return type;
    }

    public void setType(CodeSubmissionType type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }

    public Map<String,String> getEnvironment() {
        return environment;
    }

    public void setEnvironment(Map<String,String> environment) {
        this.environment = environment;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getResultFormat() {
        return resultFormat;
    }

    public void setResultFormat(String resultFormat) {
        this.resultFormat = resultFormat;
    }

    public String getResultsPath() {
        return resultsPath;
    }

    public void setResultsPath(String resultsPath) {
        this.resultsPath = resultsPath;
    }

    public CodeSubmission() {}

    public CodeSubmission(CodeSubmissionType type, String code, String className, List<String> arguments, Map<String,String> environment, String config, String resultFormat, String resultsPath) {
        this.type = type;
        this.code = code;
        this.className = className;
        this.arguments = arguments;
        this.environment = environment;
        this.config = config;
        this.resultFormat = resultFormat;
        this.resultsPath = resultsPath;
    }

    public CodeSubmissionType type() {
        return type;
    }

    public String code() {
        return code;
    }

    public String className() {
        return className;
    }

    public String config() {
        return config;
    }

    public String resultFormat() {
        return resultFormat;
    }

    public String resultsPath() {
        return resultsPath;
    }

    public void type(CodeSubmissionType type) {
        this.type = type;
    }

    public void code(String code) {
        this.code = code;
    }

    public void className(String className) {
        this.className = className;
    }

    public void config(String config) {
        this.config = config;
    }

    public void resultFormat(String resultFormat) {
        this.resultFormat = resultFormat;
    }

    public void resultsPath(String resultsPath) {
        this.resultsPath = resultsPath;
    }

    public List<String> arguments() {
        return arguments;
    }

    public void arguments(List<String> arguments) {
        this.arguments = arguments;
    }

    public Map<String,String> environment() {
        return environment;
    }

    public void environment(Map<String,String> environment) {
        this.environment = environment;
    }
}