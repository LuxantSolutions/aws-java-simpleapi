package com.luxant.sqs;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SqsConsumer extends SqsProvider implements Runnable {
    String qName;
    String qUrl;
    MessageHandler msgHandler;
    int msgCount;
    Duration reqTimeout = Duration.ofSeconds(1);
    AtomicInteger received = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(false);

    Logger logger = Logger.getGlobal();

    /**
     * @param client the SqSClient, if null one is created.
     */
    private void init(String queueName, int count, Duration requestTimeout, MessageHandler handler) {
        if (queueName == null) {
            throw new IllegalArgumentException("queueName cannot be null.");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler cannot be null.");
        }
        qName = queueName;
        msgHandler = handler;
        msgCount = count;
        reqTimeout = requestTimeout;

        qUrl = createQueue(qName);
    }

    public SqsConsumer(SqsClient client, String queueName, int count, Duration requestTimeout, MessageHandler handler) {
        super(client);
        init(queueName, count, requestTimeout, handler);
    }

    /**
     * @param client the SqSClient
     */
    public SqsConsumer(String queueName, int count, Duration requestTimeout, MessageHandler handler) {
        super(null);
        init(queueName, count, requestTimeout, handler);
    }

    public void deleteMessage(Message m) {
        Utils.deleteMessage(sqsClient, qUrl, m);
    }

    private void handleMessage(Message m) {
        try {
            msgHandler.onMsg(m);
            Utils.deleteMessage(sqsClient, qUrl, m);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Message handling exception: {0}", e.getMessage());
        }
    }

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

    public String getQueueUrl() {
        return qUrl;
    }

    public int getReceivedCount() {
        return received.get();
    }

    public void shutdown() {
        running.set(false);
    }
}
