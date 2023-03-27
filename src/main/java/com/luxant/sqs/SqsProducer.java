package com.luxant.sqs;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsProducer extends SqsProvider {

    AtomicInteger sentCount = new AtomicInteger(0);

    Logger logger = Logger.getGlobal();
    HashMap<String, SendMessageRequest> qUrlMap = new HashMap<>();
    Object lock = new Object();

    /**
     * Creates a SqsProducer to send messages to a queue.
     * A SqsClient will be created specific to this producer.
     * 
     * @param queueName - the name of the queue
     */
    public SqsProducer() {
        super(null);
    }

    public SqsProducer(SqsClient client) {
        super(client);
    }

    private SendMessageRequest getBaseSMR(String queueName) {
        synchronized (lock) {
            var baseSmr = qUrlMap.get(queueName);
            if (baseSmr == null) {
                String qUrl = Utils.createQueue(sqsClient, queueName);
                baseSmr = SendMessageRequest.builder()
                        .queueUrl(qUrl)
                        .delaySeconds(0)
                        .build();
                qUrlMap.put(queueName, baseSmr);
            }
            return baseSmr;
        }
    }

    public void sendMessage(String queueName, String body) {
        sqsClient.sendMessage(getBaseSMR(queueName).toBuilder().messageBody(body).build());
        sentCount.incrementAndGet();
    }

    public int getSentCount() {
        return sentCount.get();
    }

    public String getQueueUrl(String queueName) {
        return getBaseSMR(queueName).queueUrl();
    }
}
