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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SqsProducer accepts or creates a SqsClient and sends
 * messages.
 */
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

    /**
     * Creates a SqsProducer to send messages to a queue, using
     * the provided SqsClient.
     * @param client the SqsClient.
     */
    public SqsProducer(SqsClient client) {
        super(client);
    }

   
   /** 
    * Creates or gets a cached send message request for a specific queue.
    * @param queueName - name of the queue to send to.
    * @return SendMessageRequest withi the queue name.
    */
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

    /**
     * Sends a message with a string body to the specified queue.
     * @param queueName - Name of the queue to send the message to.
     * @param body - the payload of the message.
     * @throws InvalidMessageContentsException
     *         The message contains characters outside the allowed set.
     * @throws UnsupportedOperationException
     *         Error code 400. Unsupported operation.
     * @throws SdkException
     *         Base class for all exceptions that can be thrown by the SDK (both service and client). Can be used for
     *         catch all scenarios.
     * @throws SdkClientException
     *         If any client side error occurs such as an IO related failure, failure to get credentials, etc.
     * @throws SqsException
     *         Base class for all service exceptions. Unknown exceptions will be thrown as an instance of this type.
     */
    public void sendMessage(String queueName, String body) {
        sqsClient.sendMessage(getBaseSMR(queueName).toBuilder().messageBody(body).build());
        sentCount.incrementAndGet();
    }

    /**
     * Gets the number of messages this SqsProducer instance has sent.
     * @return number of messages sent
     */
    public int getSentCount() {
        return sentCount.get();
    }

    /**
     * Gets the base queue name for the url. This is a convenience function
     * for using other SqS APIs with this one without having to perform a lookup
     * from the queue name.
     * 
     * @param queueName name of the SqsQueue
     * @return the SQS Queue URl.
     */
    public String getQueueUrl(String queueName) {
        return getBaseSMR(queueName).queueUrl();
    }
}
