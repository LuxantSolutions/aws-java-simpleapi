package com.luxant.sqs;

import org.junit.Test;

import static org.junit.Assert.*;


public class SqsProducerTests {

    @Test public void TestBasicProducer() {
        SqsProducer p = new SqsProducer();
        p.sendMessage("TestBasicProducerQueue", "hello");
        p.sendMessage("TestBasicProducerQueue2", "hello");

        String qUrl1 = Utils.getQueueUrl(p.getClient(), "TestBasicProducerQueue");
        String qUrl2 = Utils.getQueueUrl(p.getClient(), "TestBasicProducerQueue2");
        
        var msgs = Utils.receiveMessages(p.getClient(), qUrl1, 2);
        assertEquals(1, msgs.size());
        assertEquals("hello", msgs.get(0).body());

        msgs = Utils.receiveMessages(p.getClient(), qUrl2, 2);
        assertEquals(1, msgs.size());
        assertEquals("hello", msgs.get(0).body());

        Utils.deleteQueue(p.getClient(), qUrl1);
        Utils.deleteQueue(p.getClient(), qUrl2);
    }
}
