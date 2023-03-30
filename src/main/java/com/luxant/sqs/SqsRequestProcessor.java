// Copyright 2023 Luxant Solutions
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.luxant.sqs;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Internal class used to process incoming requests.  This class 
 * basically creates a temporary queue (that it will clean up),
 * and a hash of outstanding requests. When a request is sent, 
 * a future of the response is hashed with an response ID.
 * The responder is required to send the response with the proper
 * request ID.  When the response arrives, the future is obtained
 * from the hash and completed. 
 */
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

    String nextResponseID() {
        int c = requestCount.incrementAndGet();
        return Long.toString(c) + requestorUUID;
    }

    SqsRequestProcessor(SqsClient client, String name, String qName) {
        appName = name;
        String qn = qName;
        if (qn == null) {
            qn = appName + "-" + requestorUUID;
            ownsQueue = true;
        }

        qUrl = Utils.createQueue(client, qn);
        
        consumer = new SqsConsumer(client, qn, -1, Duration.ofSeconds(2), new ResponseHandler(), true);
        executor.execute(consumer);
    }

    String getQueueUrl() {
        return consumer.getQueueUrl();
    }

    CompletableFuture<String> addRequest(String rid, CompletableFuture<String> f) {
        synchronized (chmLock) {
            chm.put(rid, f);
        }
        return f;
    }

    void removeRequest(String rid) {
        synchronized (chmLock) {
            chm.remove(rid);
        }
    }

    String getReplyQueueUrl() {
        return qUrl;
    }

    class ResponseHandler implements Consumer<Message> {

        @Override
        public void accept(Message m) {
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

    void shutdown() {
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
