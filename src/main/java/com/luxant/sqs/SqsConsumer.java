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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * The SqsConsumer consumes messages invoking a provided handler
 * for every message received.
 */
public class SqsConsumer extends SqsProvider implements Runnable {
    String qName;
    String qUrl;
    Consumer<Message> msgCons;
    int msgCount;
    boolean deleteMsg = true;
    Duration reqTimeout = Duration.ofSeconds(1);
    AtomicInteger received = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(false);

    Logger logger = Logger.getGlobal();

    private void init(String queueName, int count, Duration requestTimeout, Consumer<Message> handler, boolean delete) {
        if (queueName == null) {
            throw new IllegalArgumentException("queueName cannot be null.");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler cannot be null.");
        }
        qName = queueName;
        msgCons = handler;
        msgCount = count;
        reqTimeout = requestTimeout;
        deleteMsg = delete;

        qUrl = createQueue(qName);
    }

    /**
     * Creates a SqsConsumer.
     * @param client a SqsClient.  Null will create a new SqsClient.
     * @param queueName name of the queue to consume messages from
     * @param count number of messages to process before exiting. -1 is unlimited.
     * @param requestTimeout - internal timeout indicating how often to request messages.
     * @param msgConsumer - invoked every time a messages is received.
     */
    public SqsConsumer(SqsClient client, String queueName, int count, Duration requestTimeout, Consumer<Message> msgConsumer, boolean deleteMsg) {
        super(client);
        init(queueName, count, requestTimeout, msgConsumer, deleteMsg);
    }

    /**
     * Creates a SqsConsumer.
     * @param queueName name of the queue to consume messages from
     * @param count number of messages to process before exiting. -1 is unlimited.
     * @param requestTimeout - internal timeout indicating how often to request messages.
     * @param msgConsumer - invoked every time a messages is received.
     */
    public SqsConsumer(String queueName, int count, Duration requestTimeout, Consumer<Message> msgConsumer) {
        super(null);
        init(queueName, count, requestTimeout, msgConsumer, true);
    }

    /**
     * Creates a SqsConsumer.
     * @param queueName name of the queue to consume messages from
     * @param msgConsumer - invoked every time a messages is received.
     */
    public SqsConsumer(String queueName, Consumer<Message> msgConsumer) {
        super(null);
        init(queueName, -1, Duration.ofSeconds(2), msgConsumer, true);
    }    

    /**
     * Deletes a message received by this consumer.
     * @param m the message to delete.
     */
    public void deleteMessage(Message m) {
        Utils.deleteMessage(sqsClient, qUrl, m);
    }

    private void handleMessage(Message m) {
        try {
            msgCons.accept(m);
            if (deleteMsg) {
                deleteMessage(m);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Message handling exception: {0}", e.getMessage());
        }
    }

    /** 
     * Receives and processes messages until one of the following occurs:
     * SqsConsumer.shutdown() is called, the thread is interrupted or the number of messages specified have been received.
     */
    @Override
    public void run() {
        running.set(true);

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(qUrl)
                .maxNumberOfMessages(10)
                .visibilityTimeout(10)
                .waitTimeSeconds((int) reqTimeout.getSeconds())
                .messageAttributeNames("ALL", SqsRequestor.RESPONSE_ID, SqsRequestor.RESPONSE_QUEUE)
                .build();

        // This is a sawtooth pattern here, but with Sqs rates an
        // optimization wouldn't make much of a difference unless
        // the callback was very slow and user code blocked.
        // In a high speed system, you'd optimize this to continuously
        // poll and fill a buffer in which another thread would invoke
        // the callback.
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                var msgs = sqsClient.receiveMessage(receiveMessageRequest).messages();
                for (Message m : msgs) {
                    received.getAndIncrement();
                    handleMessage(m);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "SQS Receive exception: {0}", e.getMessage());
                break;
            }

            // -1 runs forever
            if (msgCount > 0 && received.get() >= msgCount) {
                running.set(false);
            }
        }
        var logline = String.format("Exiting consumer on queue %s with %d msgs.", qName, received.get());
        logger.log(Level.FINE, logline);
    }

    /**
     * Convenience function to get the queue url.
     * @return url of the queue.
     */
    public String getQueueUrl() {
        return qUrl;
    }

    /**
     * Gets the number of messages received.
     * @return number of messages
     */
    public int getReceivedCount() {
        return received.get();
    }

    /*
     * Gracefully shuts down processing to avoid interrupting 
     * the thread to exit run().
     */
    public void shutdown() {
        running.set(false);
    }
}
