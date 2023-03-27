package com.luxant.sqs;

import java.util.HashMap;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

public abstract class SqsProvider {

    protected SqsClient sqsClient;
    private HashMap<String, String> queues = new HashMap<>();
    private Object lock = new Object();

    SqsProvider(SqsClient c) {
        sqsClient = (c == null) ? Utils.createClient() : c;
    }

    public SqsClient getClient() {
        return sqsClient;
    }

    public String createQueue(String queueName) {
        synchronized(lock) {
            String url = Utils.createQueue(sqsClient, queueName);
            queues.put(queueName, url);
            return url;
        }
    }

    public void deleteQueue(String queueName) {
        synchronized(lock) {
            String url = queues.get(queueName);
            if (url == null) {
                return;
            }
            sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build());
            queues.remove(queueName);
        }
    }

    public void purgeQueue(String queueName) {
        synchronized(lock) {
            String url = queues.get(queueName);
            if (url == null) {
                return;
            }
            sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(url).build());
        } 
    }
    
    public void cleanup() {
        synchronized(lock) {
            var urls = queues.keySet();
            for (String u : urls) {
                // TODO: add option to deleteQueue(u);
                purgeQueue(u);
            }
        }
    }
}
