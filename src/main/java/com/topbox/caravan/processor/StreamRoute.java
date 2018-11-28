package com.topbox.caravan.processor;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.DBObject;
import com.topbox.analytic.elasticsearch.model.Transcription;
import com.topbox.rci.dao.repository.TimelineRepository;
import com.topbox.transcription.service.TranscriptionService;
import com.topbox.util.JsonUtils;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "concurrency")
public class StreamRoute extends RouteBuilder {

    protected Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    private static final String SEDA_ENDPOINT_TEMPLATE = "seda:%s?concurrentConsumers=%s&blockWhenFull=true&size=%s&timeout=0&waitForTaskToComplete=never";

    @Autowired
    private ResultsSink resultSink;

    @Autowired
    private TranscriptionService transcriptionService;

    @Autowired
    private TimelineRepository timelineRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    private RestTemplate restTemplate;
    private JsonUtils jsonUtils;

    private Integer processCorePoolSize;
    private Integer processMaxPoolSize;
    private Integer processQueueCapacity;
    private Integer s3FetchCorePoolSize;
    private Integer s3FetchMaxPoolSize;
    private Integer s3FetchQueueCapacity;
    private Integer bulkIndexBatchSize;

    @PostConstruct
    public void initializeRestTemplate() {
        int timeout = 5000;
        HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        clientHttpRequestFactory.setConnectTimeout(timeout);
        clientHttpRequestFactory.setConnectionRequestTimeout(timeout);
        clientHttpRequestFactory.setReadTimeout(timeout);
        restTemplate = new RestTemplate(clientHttpRequestFactory);
    }

    @PostConstruct
    public void initializeJsonUtils() {
        this.jsonUtils = JsonUtils.getInstance();
    }

    @Override
    public void configure() throws Exception {

        String fetchS3TranscritionSedaEndpoint = getSedaEndpoint("fetchS3", s3FetchCorePoolSize,
            s3FetchQueueCapacity);
        String processSedaEndpoint = getSedaEndpoint("process", processCorePoolSize, processQueueCapacity);

        onException(Exception.class).setHeader(ResultsSink.KEY).constant(ResultsSink.ERROR)
            .to("bean:resultSink?method=incrementCount(${header.key}, ${header.TASK_ID})")
            .to("log:error?showCaughtException=false&showStackTrace=false");

        from("direct:count").to(
            "mongodb:mongoClient?database=production&collection=timelines&operation=count&readPreference=secondary");

        from("direct:start")
            .to("mongodb:mongoClient?database=production&collection=timelines&operation=findAll&outputType=DBCursor&readPreference=secondary")
            .setHeader("ENGAGEMENT_ID").simple("${in.header.ENGAGEMENT_ID}")
            .setHeader("TASK_ID").simple("${in.header.TASK_ID}")
            .split(body()).streaming()
            .process(new Processor() {
                public void process(Exchange exchange) throws Exception {
                    exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                    exchange.getOut().setBody(exchange.getIn().getBody());
                }
            })
            .to(fetchS3TranscritionSedaEndpoint);

        from(fetchS3TranscritionSedaEndpoint)
            .process(new Processor() {
                public void process(Exchange exchange) throws Exception {
                    log.debug("starting FetchTranscriptCamelProcessor.process");
                    Object[] out = new Object[2];
                    String transcriptionS3UrlPrefix = (String) exchange.getIn()
                        .getHeader("S3_TRANSCRIPTIONS_URL_PATH");

                    DBObject timeline = (DBObject) exchange.getIn().getBody();
                    if (timeline != null) {
                        boolean isClonedTimeline = timeline.containsField("clonedFrom");
                        DBObject timeline_fileName = (DBObject) timeline.get("fileName");
                        if (timeline_fileName != null) {
                            String uniqueIdentifier = (String) timeline_fileName.get("uniqueIdentifier");
                            if (isClonedTimeline) {
                                uniqueIdentifier = StringUtils.substringBeforeLast(uniqueIdentifier, "___");
                            }
                            Transcription transcript = transcriptionService.getTranscriptionFromS3(
                                transcriptionS3UrlPrefix + uniqueIdentifier + ".json",
                                null);
                            out[0] = transcript.getBodyFull();
                        }
                    }
                    out[1] = (String) timeline.get("topBoxId");
                    exchange.getOut().setBody(out);
                    exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                    log.debug("ending FetchTranscriptCamelProcessor.process");
                }
            })
            .to(processSedaEndpoint);

        from(processSedaEndpoint)
            .process(new Processor() {
                public void process(Exchange exchange) throws Exception {
                    exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                    Object[] in = exchange.getIn().getBody(Object[].class);
                    String text = (String) in[0];
                    String topBoxId = (String) in[1];
                    if (StringUtils.isNotBlank(text) && StringUtils.isNotBlank(topBoxId)) {

                        String uri = (String) exchange.getIn()
                            .getHeader("CLASSIFY_URI");

                        Map<String, String> requestBody = new HashMap<>();
                        requestBody.put("text", text);
                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType(MediaType.APPLICATION_JSON);
                        HttpEntity<Map<String, String>> entity = new HttpEntity<>(requestBody, headers);
                        ResponseEntity<String> response = restTemplate.postForEntity(uri,
                            entity, String.class);
                        Map<String, Object> results = jsonUtils.toObject(response.getBody(),
                            new TypeReference<HashMap<String, Object>>() {});
                        Map<String, Object> updates = new HashMap<>();
                        updates.put("sentimentML", results);
                        timelineRepository.updateTimelineByTopBoxId(topBoxId, updates);
                        resultSink.incrementCount(ResultsSink.SUCCESS,
                            (String) exchange.getIn().getHeader(Reclassify.TASK_ID));
                    }
                    else {
                        resultSink.incrementCount(ResultsSink.ERROR,
                            (String) exchange.getIn().getHeader(Reclassify.TASK_ID));
                    }
                }
            });
    }

    private String getSedaEndpoint(String name, Integer concurrentConsumers, Integer queueSize) {
        if (StringUtils.isBlank(name)) {
            name = RandomStringUtils.random(10, true, false);
        }
        if (concurrentConsumers == null) {
            concurrentConsumers = 1;
        }
        if (queueSize == null) {
            queueSize = Integer.MAX_VALUE;
        }
        String uri = String.format(SEDA_ENDPOINT_TEMPLATE, name, concurrentConsumers, queueSize);
        log.info("created seda endpoint url for name: {}, {}", name, uri);
        return uri;
    }

    public Integer getS3FetchCorePoolSize() {
        return s3FetchCorePoolSize;
    }

    public void setS3FetchCorePoolSize(Integer s3FetchCorePoolSize) {
        this.s3FetchCorePoolSize = s3FetchCorePoolSize;
    }

    public Integer getS3FetchMaxPoolSize() {
        return s3FetchMaxPoolSize;
    }

    public void setS3FetchMaxPoolSize(Integer s3FetchMaxPoolSize) {
        this.s3FetchMaxPoolSize = s3FetchMaxPoolSize;
    }

    public Integer getS3FetchQueueCapacity() {
        return s3FetchQueueCapacity;
    }

    public void setS3FetchQueueCapacity(Integer s3FetchQueueCapacity) {
        this.s3FetchQueueCapacity = s3FetchQueueCapacity;
    }

    public Integer getProcessCorePoolSize() {
        return processCorePoolSize;
    }

    public void setProcessCorePoolSize(Integer processCorePoolSize) {
        this.processCorePoolSize = processCorePoolSize;
    }

    public Integer getProcessMaxPoolSize() {
        return processMaxPoolSize;
    }

    public void setProcessMaxPoolSize(Integer processMaxPoolSize) {
        this.processMaxPoolSize = processMaxPoolSize;
    }

    public Integer getProcessQueueCapacity() {
        return processQueueCapacity;
    }

    public void setProcessQueueCapacity(Integer processQueueCapacity) {
        this.processQueueCapacity = processQueueCapacity;
    }

    public Integer getBulkIndexBatchSize() {
        return bulkIndexBatchSize;
    }

    public void setBulkIndexBatchSize(Integer bulkIndexBatchSize) {
        this.bulkIndexBatchSize = bulkIndexBatchSize;
    }

}
