package com.luxant.sqs;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsRequestor extends SqsProvider implements AutoCloseable  {

    String appName;
    String respQurl;
    Duration reqTimeout = Duration.ofSeconds(1);
    SqsClient client;
    SendMessageRequest baseSmr;

    Logger logger = Logger.getGlobal();
    public static final String RESPONSE_ID = "lux-req-resp-id";
    public static final String RESPONSE_QUEUE = "lux-resp-queue-url";

    /* Singleton that wasn't built for purists */
    SqsRequestProcessor processor;

    static SqsRequestProcessor theProcessor;
    static Object sqsReqestProcessorLock = new Object();

    static SqsRequestProcessor getProcessor(SqsClient client, String appName, String qName) {
        synchronized(sqsReqestProcessorLock) {
            if (theProcessor == null) {
                theProcessor = new SqsRequestProcessor(client, appName, qName);
            }
            return theProcessor;
        }
    }

    private HashMap<String, MessageAttributeValue> getAttrs(String rid) {
        var attrs = new HashMap<String, MessageAttributeValue>();
        attrs.putAll(baseSmr.messageAttributes());
        attrs.put(RESPONSE_ID, MessageAttributeValue.builder().
            dataType("String").
            stringValue(rid).
            build());
        return attrs;
    }

    // fast lookup for existing queues.
    ConcurrentHashMap<String,String> qUrlMap = new ConcurrentHashMap<>();
 
    String getQueueUrl(String queueName) {
        String url = qUrlMap.get(queueName);
        if (url != null) {
            return url;
        }

        url = Utils.createQueue(sqsClient, queueName);
        qUrlMap.put(queueName, url);
        return url;
    }

    public CompletableFuture<String> request(String queueName, String body) {
        String rid = processor.nextResponseID();
        CompletableFuture<String> f = new CompletableFuture<>();
        
        // prepare the single threaded processor to look
        // for the reply using the new rid.
        processor.addRequest(rid, f);


        var smr = baseSmr.toBuilder().
            messageBody(body).
            queueUrl(getQueueUrl(queueName)).
            messageAttributes(getAttrs(rid)).
            build();

        // Kinda confusing, but we use the runAsync as a shortcut
        // to avoid blocking on the send. Upon an error, the future
        // is completed exceptionally and the rid is cleaned up.
        CompletableFuture.runAsync(() -> {
            try { 
                sqsClient.sendMessage(smr);
            } catch (Exception e) {
                processor.removeRequest(rid);
                f.completeExceptionally(e);
                logger.severe("Exception sending: " + e);
            }
        });
        return f;
    }

    public String request(String queue, String body, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
        var f = request(queue, body);
        return f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @param client the SqSClient, if null one is created.
     */
    private void init(String appName, String respQueueName, Duration requestTimeout) {
        if (appName == null) {
            throw new IllegalArgumentException("queueName cannot be null.");
        }        

        reqTimeout = requestTimeout;
        processor = getProcessor(sqsClient, appName, respQueueName);
        respQurl = processor.getQueueUrl();

        var attrs = new HashMap<String, MessageAttributeValue>();
        attrs.put(RESPONSE_QUEUE, MessageAttributeValue.builder().
             stringValue(respQurl).
             dataType("String").
             build());

        baseSmr = SendMessageRequest.builder()
            .messageAttributes(attrs)
            .delaySeconds(0)
            .build();        
    }

    SqsRequestor(SqsClient client, String appName, Duration requestTimeout) {
        super(client);
        init(appName, null, requestTimeout);
    }


    /**
     * @param client the SqSClient
     */
    public SqsRequestor(String appName) {
        super(null);
        init(appName, null, Duration.ofSeconds(20));
    }
    
    /**
     * @param client the SqSClient
     */
    SqsRequestor(String appName, String responseQueueName, Duration requestTimeout) {
        super(null);
        init(appName, responseQueueName, requestTimeout);
    }
    
    public void shutdown() {
        processor.shutdown();
    }

    @Override
    public void close() throws Exception {        
        try  {
            shutdown();
        } catch (Exception e) { /* noop */}
    }
}
