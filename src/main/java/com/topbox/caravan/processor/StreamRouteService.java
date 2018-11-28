package com.topbox.caravan.processor;

import java.util.Map;

public interface StreamRouteService {

    Map<String, Integer> start(String engagementId, String clientId,
        String s3TranscriptionPath, int daysInThePastToScan, String taskId, String uri);

}
