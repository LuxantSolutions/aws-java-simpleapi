package com.luxant.sqs;

import org.junit.Test;

import software.amazon.awssdk.services.sqs.model.Message;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SqsProducerBenchTests {

    ExecutorService executor = Executors.newFixedThreadPool(110);

    @Test public void TestBasicProducerBench() {
        SqsProducerBench p = new SqsProducerBench("TestBasicProducerBenchQueue", "hello", 100, 100, true);

        var client = p.getClient();
        var qUrl = Utils.getQueueUrl(p.getClient(), "TestBasicProducerBenchQueue");

        executor.execute(p);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // recieve at least one message.
        var msgs = new ArrayList<Message>();
        while (msgs.size() < 100) {
            msgs.addAll(Utils.receiveMessages(client, qUrl, 2));
        }

        assertEquals(100, msgs.size());
        assertEquals("hello", msgs.get(0).body());

        Utils.deleteQueue(p.getClient(), qUrl);
    }
}
