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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * A SqsRequestor sends requests to a SqsResponder and receives a response.
 */
public class SqsRequestor extends SqsProvider implements AutoCloseable  {

    String appName;
    String respQurl;
    SqsClient client;
    SendMessageRequest baseSmr;

    Logger logger = Logger.getGlobal();
    public static final String RESPONSE_ID = "lux-req-resp-id";
    public static final String RESPONSE_QUEUE = "lux-resp-queue-url";

    /* Singleton that wasn't built for purists */
    SqsRequestProcessor processor;

    static SqsRequestProcessor theProcessor;
    static Object sqsReqestProcessorLock = new Object();

    /**
     * Gets the static processor.
     * @param client SqsClient.  If null, one will be created.
     * @param appName Name of the application for use in generating a response queue.
     * @param qName Name of the queue.
     * @return a sqs requestor object.
     */
    static private SqsRequestProcessor getProcessor(SqsClient client, String appName, String qName) {
        // TODO:  Create a processor per app name for scaling within the JVM.
        synchronized(sqsReqestProcessorLock) {
            if (theProcessor == null) {
                theProcessor = new SqsRequestProcessor(client, appName, qName);
            }
            return theProcessor;
        }
    }

    /**
     * Adds the response ID to the message attributes.
     * @param rid the response ID
     * @return Hashmap of the messages attributes.
     */
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
 
    /**
     * Gets a cached queue url for a queue, will create a queue if need be.
     * @param queueName name of the queue.
     * @return
     */
    String getQueueUrl(String queueName) {
        String url = qUrlMap.get(queueName);
        if (url != null) {
            return url;
        }

        url = Utils.createQueue(sqsClient, queueName);
        qUrlMap.put(queueName, url);
        return url;
    }

    /**
     * Requests a message from a SqsResponder.
     * @param queueName name of the queue
     * @param body body of the message
     * @return Future for the result returned from SqsResponder.
     */
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

    /**
     * Requests a message from a SqsResponder blocking until a response arrives.
     * @param queueName name of the queue
     * @param body body of the message
     * @param timeout timeout for the request.
     * @return The result from the SqsResponser as a String.
     */
    public String request(String queue, String body, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
        var f = request(queue, body);
        return f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void init(String appName, String respQueueName) {
        if (appName == null) {
            throw new IllegalArgumentException("queueName cannot be null.");
        }        

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

    /**
     * Creates a SqsRequestor
     * 
     * @param client SqsClient, if null one will be created.
     * @param appName Application name for determining response queue name (if not set)
     * @param responseQueueName Override for the temporary response queue name.
     * @param requestTimeout - internal timeout for request polling.
     */
    public SqsRequestor(SqsClient client, String appName, String responseQueueName, Duration requestTimeout) {
        super(client);
        init(appName, responseQueueName);
    }

    /**
     * Creates an SqsRequestor
     * @param appName an application used to help in naming a temporary queue.
     */
    public SqsRequestor(String appName) {
        super(null);
        init(appName, null);
    }
    
    /**
     * Shuts down this requestor and frees resources.
     */
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
