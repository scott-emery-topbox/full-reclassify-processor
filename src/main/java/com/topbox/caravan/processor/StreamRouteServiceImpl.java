package com.topbox.caravan.processor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mongodb.MongoDbConstants;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

@Component
public class StreamRouteServiceImpl implements StreamRouteService {

    protected Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    @Autowired
    private ResultsSink resultsSink;

    @Produce(uri = "direct:start")
    private ProducerTemplate template;

    public Map<String, Integer> start(String engagementId, String clientId,
        String s3TranscriptionPath, int daysInThePastToScan, String taskId, String uri) {
        try {
            resultsSink.start(taskId);

            Map<String, Object> headers = new HashMap<String, Object>();

            // build mongo query object
            DBObject query = new BasicDBObject();
            query.put("clientEngagementId", new ObjectId(engagementId));
            query.put("fileName.audioReceived", true);
            query.put("deleted", false);
            query.put("itype", "Ri");

            DBObject interactionStartDateClause = new BasicDBObject("observer.interactionStartDate",
                (new BasicDBObject("$gte", DateUtils.addDays(new Date(), daysInThePastToScan * -1))));

            DBObject isObservedClause = new BasicDBObject("isObserved", true);

            BasicDBList or = new BasicDBList();
            or.add(interactionStartDateClause);
            or.add(isObservedClause);
            query.put("$or", or);

            DBObject fieldFilter = BasicDBObjectBuilder.start()
                .add("_id", 0)
                .add("fileName.uniqueIdentifier", 1)
                .add("clonedFrom", 1)
                .add("topBoxId", 1).get();

            headers.put(Reclassify.S3_TRANSCRIPTIONS_URL_PATH, s3TranscriptionPath);
            headers.put(Reclassify.CLASSIFY_URI, uri);
            headers.put(Reclassify.ENGAGEMENT_ID, engagementId);
            headers.put(Reclassify.TASK_ID, taskId);
            headers.put(Reclassify.CLIENT_ID, clientId);
            headers.put(MongoDbConstants.FIELDS_FILTER, fieldFilter);

            Long totalCount = template.requestBody("direct:count", query, Long.class);
            resultsSink.set(taskId, ResultsSink.TOTAL, totalCount.intValue());
            log.info("[taskId={}] count of timelines which will be scanned: {}", taskId, totalCount);

            template.asyncRequestBodyAndHeaders(template.getDefaultEndpoint(), query, headers);
            return resultsSink.waitWhileLoggingProgress(taskId);
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
        finally {
            resultsSink.end(taskId);
        }
    }
}
