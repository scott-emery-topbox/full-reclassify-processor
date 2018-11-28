package com.topbox.caravan.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("resultSink")
public class ResultsSink {

    protected Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    public static final int PROGRESS_LOGGGING_INTERVAL_MILLIS = 15000;

    public static final double COMPLETE_THRESHOLD = 99D;
    public static final int ERROR_THRESHOLD = 2;

    public static final String KEY = "key";

    public static final String TOTAL = "total";
    public static final String SUCCESS = "success";
    public static final String ERROR = "error";

    private final Map<String, ConcurrentHashMap<String, Integer>> map = new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();

    public synchronized void incrementCount(String key, String taskId) {
        Map<String, Integer> taskIdResults = getTaskIdResults(taskId);
        if (taskIdResults != null) {
            taskIdResults.put(key, taskIdResults.get(key) + 1);
        }
    }

    public synchronized void incrementCount(String key, String taskId, int add) {
        Map<String, Integer> taskIdResults = getTaskIdResults(taskId);
        if (taskIdResults != null) {
            taskIdResults.put(key, taskIdResults.get(key) + add);
        }
    }

    public void set(String taskId, String key, Integer value) {
        Map<String, Integer> taskIdResults = getTaskIdResults(taskId);
        taskIdResults.put(key, value);
    }

    private Map<String, Integer> getTaskIdResults(String taskId) {
        if (StringUtils.isNotBlank(taskId)) {
            return map.get(taskId);
        }
        return null;
    }

    public void start(String taskId) {
        ConcurrentHashMap<String, Integer> taskIdResults = new ConcurrentHashMap<String, Integer>();
        taskIdResults.put(ERROR, 0);
        taskIdResults.put(SUCCESS, 0);
        taskIdResults.put(TOTAL, 0);
        map.put(taskId, taskIdResults);
    }

    public void end(String taskId) {
        map.remove(taskId);
    }

    private boolean isDone(Map<String, Integer> taskIdResults) {
        return getPercentComplete(taskIdResults) >= COMPLETE_THRESHOLD;
    }

    public boolean isSuccess(String taskId) {
        return getPercentError(getTaskIdResults(taskId)) < ERROR_THRESHOLD;
    }

    private double getPercentComplete(Map<String, Integer> taskIdResults) {
        int total = taskIdResults.get(TOTAL);
        int success = taskIdResults.get(SUCCESS);
        int error = taskIdResults.get(ERROR);
        return (total == 0) ? 100 : (((success + error) / (double) total) * 100);
    }

    private int getPercentError(Map<String, Integer> taskIdResults) {
        int total = taskIdResults.get(TOTAL);
        int error = taskIdResults.get(ERROR);
        return (total == 0) ? 0 : ((int) (((error) / (double) total) * 100));
    }

    public Map<String, Integer> waitWhileLoggingProgress(String taskId) {
        Map<String, Integer> taskIdResults = getTaskIdResults(taskId);
        while (!isDone(taskIdResults)) {
            log.info("[taskId={}] completePercent: {}; success: {}, error: {}, total: {}", taskId,
                getPercentComplete(taskIdResults), taskIdResults.get(SUCCESS), taskIdResults.get(ERROR),
                taskIdResults.get(TOTAL));
            try {
                Thread.sleep(PROGRESS_LOGGGING_INTERVAL_MILLIS);
            }
            catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        isSuccess(taskId);
        taskIdResults.put("errorPercentage", getPercentError(taskIdResults));
        taskIdResults.put("successPercentage", new Double(getPercentComplete(taskIdResults)).intValue());
        return taskIdResults;
    }

}
