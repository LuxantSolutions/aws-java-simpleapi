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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * Shared utility methods, primarily used internally and in testing.
 */
public class Utils {

    private Utils() {
    }

    private static Logger logger = Logger.getGlobal();

    static SqsClient createClient() {
        return SqsClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.builder().build())
                .build();
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets the queue url for a given queue name.
     * @param client SqsClient to use
     * @param queueName name of the queue
     * @return the queue url
     */
    static String getQueueUrl(SqsClient client, String queueName) {
        var qUrlReq = GetQueueUrlRequest.builder().queueName(queueName).build();
               
        // Do not proceed until we know the queue is accessable.
        for (int i = 0; i < 10; i++) {
            try {
                var gqur = client.getQueueUrl(qUrlReq);
                if (gqur.sdkHttpResponse().isSuccessful()) {
                    return gqur.queueUrl();
                }
            } catch (QueueDoesNotExistException qdex) {
                Utils.sleep(250);
            }
        }
        return null;
    }

    private static void blockUntilQueueCreated(SqsClient client, String queueName) {
        getQueueUrl(client, queueName);
    }

    /**
     * Creates a standard Queue.
     * 
     * @param client - the SqsClient
     * @param name   - name of the Queue
     * @return the url of the queue.
     */
    static String createQueue(SqsClient client, String name) {
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null.");
        }
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null.");
        }

        EnumMap<QueueAttributeName, String> qAttributes = new EnumMap<>(QueueAttributeName.class);
        qAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, "30");
        qAttributes.put(QueueAttributeName.DELAY_SECONDS, "0");

        var cqResp = client.createQueue(
                CreateQueueRequest.builder()
                        .queueName(name)
                        .attributes(qAttributes)
                        .build());

        /* not ideal, but we really want to avoid retries elsewhere */
        blockUntilQueueCreated(client, name);

        return cqResp.queueUrl();
    }

    /**
     * Connects to AWS SQS in US_EAST_2 with the profile credentials.
     * @return SqsClient.
     */
    public SqsClient connect() {
        // TODO - read properties file for configuration.
        var pcp = ProfileCredentialsProvider.builder().build();

        return SqsClient.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(pcp)
                .build();
    }

    public void sendMessage(SqsClient sqc, String queueUrl, String payload) {
        var smr = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(payload)
                .delaySeconds(0)
                .build();

        var resp = sqc.sendMessage(smr);
        logger.log(Level.INFO, () -> "Sent msg: {0}" + resp);
    }

    public void sendMessageBatch(SqsClient sqc, String queueUrl, String payload) {
        var msgs = new SendMessageBatchRequestEntry[10];
        for (int i = 0; i < msgs.length; i++) {
            msgs[i] = SendMessageBatchRequestEntry.builder()
                    .messageBody(payload)
                    .delaySeconds(0)
                    .id(Integer.toString(i))
                    .build();
        }
        var smr = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(msgs)
                .build();

        sqc.sendMessageBatch(smr);
    }

    public static List<String> listQueues(SqsClient sqsClient) {
        return sqsClient.listQueues(ListQueuesRequest.builder().build()).queueUrls();
    }

    public static List<String> listQueuesFilter(SqsClient sqsClient, String prefix) {
        return sqsClient.listQueues(ListQueuesRequest.builder().queueNamePrefix(prefix).build()).queueUrls();
    }

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(5)
                    .build();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();

        } catch (SqsException e) {
            logger.severe(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return new ArrayList<>();
    }

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl, int sTimeout) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .visibilityTimeout(30)
                    .waitTimeSeconds(sTimeout)
                    .build();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();

        } catch (SqsException e) {
            logger.severe(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return new ArrayList<>();
    }

    public static void changeMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {

        logger.info("\nChange Message Visibility");
        try {

            for (Message message : messages) {
                ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .visibilityTimeout(100)
                        .build();
                sqsClient.changeMessageVisibility(req);
            }

        } catch (SqsException e) {
            logger.severe(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Delete a message
     */
    public static void deleteMessage(SqsClient sqsClient, String queueUrl, Message m) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {
        logger.info("\nDelete Messages");

        try {
            for (Message m : messages) {
                deleteMessage(sqsClient, queueUrl, m);
            }
        } catch (SqsException e) {
            logger.severe(e.awsErrorDetails().errorMessage());
        }
    }

    public static void setLoggerFormat() {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    public static void deleteQueue(SqsClient client, String queueUrl) {
        var dqr = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
        client.deleteQueue(dqr);
    }

    public static void deleteQueue(String queueName) {
        var client = Utils.createClient();
        var queueUrl = getQueueUrl(client, queueName);
        var dqr = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
        client.deleteQueue(dqr);
    }    
}
