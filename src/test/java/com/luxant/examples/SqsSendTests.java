package com.luxant.examples;

import static org.junit.Assert.assertEquals;

import java.time.Duration;

import org.junit.Test;

import com.luxant.sqs.SqsConsumer;
import com.luxant.sqs.Utils;

public class SqsSendTests {

    @Test
    public void testSqsSend() {
        var sender = new SqsSend();
        sender.sendMessage("test-queue-sqssend", "hello");
        
        var consumer = new SqsConsumer("test-queue-sqssend", 1,Duration.ofSeconds(1), msg-> { /*noop*/});
        consumer.run();
        assertEquals(1, consumer.getReceivedCount());
        Utils.deleteQueue(consumer.getClient(), consumer.getQueueUrl());
    }
}
