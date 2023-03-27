package com.luxant.sqs;

import org.junit.Test;

import software.amazon.awssdk.services.sqs.model.Message;

import static org.junit.Assert.*;

import java.time.Duration;


public class SqsConsumerTests {

    @Test public void TestBasicConsumer() {
        
        final String qn = "TestBasicConsumerQueue";

        class ConsumerHandler implements MessageHandler {
            public String result;

            @Override
            public void onMsg(Message m) {
                result = m.body();
            }
        }

        var ch = new ConsumerHandler();
        SqsConsumer c = new SqsConsumer(qn, 2, Duration.ofSeconds(2), ch);
        var p = new SqsProducer();
        p.sendMessage(qn, "hello #1");
        p.sendMessage(qn, "hello #2");

        c.run();

        assertEquals(2, c.getReceivedCount());
        assertTrue(ch.result.startsWith("hello"));

        Utils.deleteQueue(c.sqsClient, c.getQueueUrl());
    }
}
