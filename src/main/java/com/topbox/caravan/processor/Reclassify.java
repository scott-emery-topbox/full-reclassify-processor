package com.topbox.caravan.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import com.topbox.rci.dao.bo.Client;
import com.topbox.rci.dao.bo.clientengagement.ClientEngagement;
import com.topbox.rci.dao.repository.ClientEngagementRepository;
import com.topbox.rci.dao.repository.ClientRepository;
import com.topbox.rci.service.SearchConfigurationService;
import com.topbox.task.processor.camel.processor.AbstractCamelProcessor;
import com.topbox.task.processor.exception.WorkflowProcessorException;
import com.topbox.task.queue.bo.TaskMessage;
import com.topbox.transcription.service.TranscriptionService;

@Configuration()
@EnableCaching
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class, MongoAutoConfiguration.class,
    MongoDataAutoConfiguration.class })
@ComponentScan({ "com.topbox.dao", "com.topbox.service", "com.topbox.rci" })
@EnableConfigurationProperties
@ConfigurationProperties()
@Component("workflowProcessor")
public class Reclassify extends AbstractCamelProcessor {

    protected Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    // process options
    public static final String ENGAGEMENT_ID = "ENGAGEMENT_ID";
    public static final String S3_TRANSCRIPTIONS_URL_PATH = "S3_TRANSCRIPTIONS_URL_PATH";
    public static final String CLASSIFY_URI = "CLASSIFY_URI";

    // route constants
    public static final String CLIENT_ID = "CLIENT_ID";
    public static final String TASK_ID = "TASK_ID";
    public static final String TOP_BOX_ID = "TOP_BOX_ID";
    public static final String FILE_NAME = "FILE_NAME";
    public static final String DURATION = "DURATION";

    @Autowired
    private ClientEngagementRepository clientEngagementRepository;

    @Autowired
    private ClientRepository clientRepository;

    @Autowired
    private StreamRouteService streamRouteService;

    @Autowired
    private TranscriptionService transcriptionService;

    @Autowired
    private SearchConfigurationService searchConfigurationService;

    public String process(TaskMessage taskMessage) {
        StopWatch sw = new StopWatch();

        Map<String, Object> processOptionsEvaluated = new HashMap<String, Object>();

        String engagementId = getRequiredProcessOptionAsString(taskMessage, ENGAGEMENT_ID);
        processOptionsEvaluated.put(ENGAGEMENT_ID, engagementId);

        ClientEngagement engagement = clientEngagementRepository.findById(new ObjectId(engagementId))
            .orElseThrow(() -> new WorkflowProcessorException(
                "Unable to find clientEngagement document in Mongo using _id : " + engagementId));

        Client client = clientRepository.findById(engagement.getClientId())
            .orElseThrow(() -> new WorkflowProcessorException(
                "Unable to find client document in Mongo using _id : " + engagement.getClientId().toString()));

        String s3TranscriptionsUrlPath = (String) taskMessage.getProcessOption(S3_TRANSCRIPTIONS_URL_PATH);
        if (StringUtils.isNotBlank(s3TranscriptionsUrlPath) && !StringUtils.endsWith(s3TranscriptionsUrlPath, "/")) {
            s3TranscriptionsUrlPath = s3TranscriptionsUrlPath + "/";
        }
        processOptionsEvaluated.put(S3_TRANSCRIPTIONS_URL_PATH, s3TranscriptionsUrlPath);
        if (StringUtils.isBlank(s3TranscriptionsUrlPath)) {
            s3TranscriptionsUrlPath = String
                .format("s3://topbox-extract-%s/%s/transcripts/", client.getName(), engagement.getName())
                .toLowerCase().replaceAll(" ", "-");
            log.info("s3TranscriptionsUrlPath = {}", s3TranscriptionsUrlPath);
            processOptionsEvaluated.put(S3_TRANSCRIPTIONS_URL_PATH, s3TranscriptionsUrlPath);
        }

        String classifyUri = getRequiredProcessOptionAsString(taskMessage, CLASSIFY_URI);
        
        sw.start("reclassify");
        log.info("[taskId={}][engagementId={}] Starting reclassify", taskMessage.getTaskId(),
            engagementId);
        Map<String, Integer> resultCounts = null;
        try {
            resultCounts = streamRouteService.start(engagementId, engagement.getClientId().toString(),
                s3TranscriptionsUrlPath, engagement.getTfeConfiguration().getDaysInThePastToScan(),
                Long.toString(taskMessage.getTaskId()), classifyUri);
            sw.stop();
        }
        catch (Exception e) {
            throw new WorkflowProcessorException(e.getMessage(), e);
        }

        taskMessage.getHeaderOut().put("total_runtimeseconds", sw.getTotalTimeSeconds());
        taskMessage.getHeaderOut().put("resultCounts", resultCounts);
        log.info("{}\n{}", sw.prettyPrint(), jsonUtils.toJsonPretty(taskMessage.getHeaderOut()));
        return "DEFAULT";
    }

}
