package com.luxant.sqs;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SqsResponder extends SqsConsumer {

    public SqsResponder(SqsClient client, String queueName, int count, Duration timeout, MessageHandler handler) {
        super(client, queueName, count, timeout, handler);
    }

    public SqsResponder(String queueName, MessageHandler handler) {
        super(null, queueName, Integer.MAX_VALUE, Duration.ofSeconds(20), handler);
    }
    
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
     * To be call in onMsg as a convenience function.
     * @param m
     * @param response
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
