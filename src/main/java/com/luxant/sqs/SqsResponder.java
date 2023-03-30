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
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * The SqsRequestor class makes requests to services using the SqsResponder.
 * 
 * An easy way to use this class is as follows:
 * <pre>
 *     class MyService implements Consumer<Message>, Runnable {
 *      SqsResponder responder;
 * 
 *       public MyService(String listenQueue) {
 *           responder = new SqsResponder(listenQueue, this);
 *       }
 *
 *      @Override
 *      public void accept(Message m) {
 *          // Do some work here - this is where your application code
 *          // will process the incoming messages.  Then just use the 
 *          // convenience API to reply to service requests
 *          // with the responder. Reply with a String.
 *           responder.reply(m, "Here's your result.");
 *       }
 * 
 *      @Override
 *      public void run() {
 *          responder.run();
 *      }
 *  }
 * 
 *  // start your service
 *  ExecutorService executor = Executors.newSingleThreadExecutor();
 *  executor.execute(new MyService("my-queue"));
 * </pre>
 * 
 * The onMsg implementation will be invoked for every message received on "my-queue".
 */
public class SqsResponder extends SqsConsumer {

    /**
     * Creates a SqsResponder to handle requests generated from a SqsRequestor.
     * @param client SqsClient, if null, a default client is created.
     * @param queueName name of the queue to poll
     * @param count number of messages to process before exiting, 01 is infinite.
     * @param timeout internal poll timeout
     * @param msgConsumer the message handler.
     */
    public SqsResponder(SqsClient client, String queueName, int count, Duration timeout, Consumer<Message> msgConsumer) {
        super(client, queueName, count, timeout, msgConsumer, true);
    }

    /**
     * Creates a SqsResponder to handle requests generated from a SqsRequestor.
     * @param queueName name of the queue to poll
     * @param handler the message handler.
     */
    public SqsResponder(String queueName, Consumer<Message> msgConsumer) {
        super(null, queueName, Integer.MAX_VALUE, Duration.ofSeconds(20), msgConsumer, true);
    }
    
    /**
     * Checks that the require attributes are present in an incoming message.
     * @param attrs SQS message attributes
     * @return true if required parameters are present, false otherwise.
     */
    private boolean hasRequiredAttributes(Map <String, MessageAttributeValue> attrs) {
        if (attrs == null || attrs.isEmpty()) {
            logger.log(Level.WARNING, "Missing attributes.");
            return false;
        }
        if (!attrs.containsKey(SqsRequestor.RESPONSE_ID)) {
            logger.log(Level.WARNING, "Missing attribute {0} in received message.", SqsRequestor.RESPONSE_ID);
            return false;
        }
        if (!attrs.containsKey(SqsRequestor.RESPONSE_QUEUE)) {
            logger.log(Level.WARNING, "Missing attribute {0} in received message.", SqsRequestor.RESPONSE_QUEUE);
            return false;
        }
        return true;
    }
    
    /**
     * To be called in onMsg as a convenience function.
     * @param m the redeived message.
     * @param responseBody body of the response to be sent back to the requestor.
     */
    public void reply(Message m, String responseBody) {
        var attrs = m.messageAttributes();
        if (!hasRequiredAttributes(attrs)) {
            logger.log(Level.WARNING, "Ignoring message");
        }
        var responseID = attrs.get(SqsRequestor.RESPONSE_ID).stringValue();
        var responseQueueUrl = attrs.get(SqsRequestor.RESPONSE_QUEUE).stringValue();

        var respAttrs = new HashMap<String, MessageAttributeValue>();
        respAttrs.put(SqsRequestor.RESPONSE_ID, MessageAttributeValue.builder().dataType("String").stringValue(responseID).build());

        var smr = SendMessageRequest.builder().
            queueUrl(responseQueueUrl).
            messageBody(responseBody).
            messageAttributes(respAttrs).
            build();

        try {
            sqsClient.sendMessage(smr);
        } catch (SqsException sqse) {
            final var exlog = String.format("Reply error on queue %s: %s", responseQueueUrl, sqse.awsErrorDetails().errorMessage());
            logger.log(Level.WARNING, exlog);
            // Log, but do not error out.  If there was a an earlier requestor using
            // a temporary queue, this can be expected - we want to continue
            // processing messages.
        }
    }
}
