package com.luxant.examples;

import static org.junit.Assert.assertEquals;

import java.time.Duration;

import org.junit.Test;

import com.luxant.sqs.SqsConsumer;
import com.luxant.sqs.Utils;

public class SqsReceiveTests {

    @Test
    public void testSqsReceive() {
        var sender = new SqsSend();
        sender.sendMessage("test-queue-sqsreceive", "hello");
        sender.sendMessage("test-queue-sqsreceive", "hello");

        var c = new SqsConsumer("test-queue-sqsreceive", 2,
            Duration.ofSeconds(5), msg -> { /* noop */} );
        c.run();
        
        assertEquals(2, c.getReceivedCount());
        Utils.deleteQueue(c.getClient(), c.getQueueUrl());
    }
}
