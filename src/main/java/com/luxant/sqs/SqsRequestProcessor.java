package com.luxant.sqs;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

class SqsRequestProcessor implements AutoCloseable {
    private String appName;
    private SqsConsumer consumer;
    private boolean ownsQueue = false;

    /* Each object instance gets it own ID and counter to avoid collisions */
    String requestorUUID = UUID.randomUUID().toString();
    AtomicInteger requestCount = new AtomicInteger(0);
    String qUrl = null;
    HashMap<String,CompletableFuture<String>> chm = new HashMap<>();
    private Object chmLock = new Object();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    public String nextResponseID() {
        int c = requestCount.incrementAndGet();
        return Long.toString(c) + requestorUUID;
    }

    public SqsRequestProcessor(SqsClient client, String name, String qName) {
        appName = name;
        String qn = qName;
        if (qn == null) {
            qn = appName + "-" + requestorUUID;
            ownsQueue = true;
        }

        qUrl = Utils.createQueue(client, qn);
        
        consumer = new SqsConsumer(client, qn, Integer.MAX_VALUE, Duration.ofSeconds(20), new ResponseHandler());
        executor.execute(consumer);
    }

    public String getQueueUrl() {
        return consumer.getQueueUrl();
    }

    public CompletableFuture<String> addRequest(String rid, CompletableFuture<String> f) {
        synchronized (chmLock) {
            chm.put(rid, f);
        }
        return f;
    }

    public void removeRequest(String rid) {
        synchronized (chmLock) {
            chm.remove(rid);
        }
    }

    String getReplyQueueUrl() {
        return qUrl;
    }

    public class ResponseHandler implements MessageHandler {

        @Override
        public void onMsg(Message m) {
            var attributes = m.messageAttributes();
            var ridAttrValue = attributes.get(SqsRequestor.RESPONSE_ID);

            // shouldn't hit this, but protect against injection.
            if (ridAttrValue == null) {
                return;
            }

            synchronized (chmLock) {
                String rid = ridAttrValue.stringValue();
                var f = chm.get(rid);
                if (f != null) {
                    // we could complete outside of the lock for performance,
                    // but this is OK for the first pass.
                    f.complete(m.body());
                    chm.remove(rid);
                }
            }
        }
    }

    public void shutdown() {
        consumer.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(0, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (ownsQueue) {
            Utils.deleteQueue(consumer.sqsClient, qUrl);
        }
    }

    @Override
    public void close() throws Exception {        
        try  {
            shutdown();
        } catch (Exception e) { /* noop */}
    }
}
