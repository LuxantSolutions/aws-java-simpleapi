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

import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.services.sqs.model.Message;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class SqsPatternTests {

    Logger logger = Logger.getGlobal();

    void shutdownAndWait(ExecutorService executorService) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(20, TimeUnit.SECONDS);
            executorService = Executors.newFixedThreadPool(110);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testQueueOneToOne() {
        ExecutorService es = Executors.newFixedThreadPool(110);

        SqsConsumer sqsc = new SqsConsumer("simple-queue",
            10, Duration.ofSeconds(2),
                msg -> { /* noop */ });

        SqsProducerBench sqsp = new SqsProducerBench("simple-queue", "message", 100, 10, false);
        es.execute(sqsc);
        es.execute(sqsp);

        shutdownAndWait(es);

        assertEquals(sqsc.getReceivedCount(), sqsp.getSentCount());
        Utils.deleteQueue(sqsp.getClient(), sqsp.getQueueUrl());
    }

    @Test
    public void testQueueCompetingConsumer() {
        ExecutorService es = Executors.newFixedThreadPool(20);
        /*
         * Start filling a queue with 100 messages and then start consumers to
         * compete.
         */
        var producer = new SqsProducerBench("test-competing-consumer", "message", 10000, 100, true);
        es.execute(producer);
        /*
         * Setup the consumers. Count is 100 to avoid potential redelivery
         * and thus false negatives without large timeouts. Aggregate consumer
         * received message count should match # sent, but no one consumer should
         * receive all messages.
         */
        SqsConsumer[] consumers = new SqsConsumer[10];
        for (int i = 0; i < consumers.length; i++) {
            consumers[i] = new SqsConsumer("test-competing-consumer", 100, Duration.ofSeconds(5),
                    msg -> { /* noop */ });
            es.execute(consumers[i]);
        }

        shutdownAndWait(es);

        /* check for distribution and that all messages were delivered */
        int recvCount = 0;
        for (var c : consumers) {
            recvCount += c.getReceivedCount();
            assertNotEquals(c.getReceivedCount(), producer.getSentCount());
        }
        assertEquals(recvCount, producer.getSentCount());

        Utils.deleteQueue(producer.getClient(), producer.getQueueUrl());
    }

    @Test
    public void testQueueAggregate() {
        ExecutorService es = Executors.newFixedThreadPool(110);
        /*
         * Setup one many publishers to aggregage data and one consumer.
         */
        SqsProducerBench[] producers = new SqsProducerBench[10];
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new SqsProducerBench("test-queue-aggregate", "message", 10000, 10, true);
            es.execute(producers[i]);
        }

        var consumer = new SqsConsumer("test-queue-aggregate", 100, Duration.ofSeconds(5),
                msg -> {
                    /* noop */});
        es.execute(consumer);

        shutdownAndWait(es);

        /* check for distribution and that all messages were delivered */
        int sendCount = 0;
        for (var p : producers) {
            sendCount += p.getSentCount();
        }
        assertEquals(consumer.getReceivedCount(), sendCount);
        Utils.deleteQueue(consumer.getClient(), consumer.getQueueUrl());
    }

    private class MyService implements Consumer<Message>, Runnable {
        static final public String RESPONSE_BODY = "Here's some help.";

        SqsResponder responder;
        private int delay;

        public MyService(String listenQueue, int workDelay) {
            responder = new SqsResponder(listenQueue, this);
            delay = workDelay;
        }

        @Override
        public void accept(Message m) {
            if (delay > 0) {
                Utils.sleep(delay);
            }
            responder.reply(m, RESPONSE_BODY);
        }

        @Override
        public void run() {
            responder.run();
        }

        public int getReceivedCount() {
            return responder.getReceivedCount();
        }
    }

    @Test
    public void testRequestReplySerial() {
        ExecutorService es = Executors.newFixedThreadPool(10);

        // launch the simple service
        es.execute(new MyService("service-queue", 0));

        /* make a request */
        try (SqsRequestor requestor = new SqsRequestor("requestor-serial")) {
            for (int i = 0; i < 10; i++) {
                String response = requestor.request("service-queue", "help!", Duration.ofSeconds(10));
                assertEquals(MyService.RESPONSE_BODY, response);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown: " + e.getMessage());
        }
        Utils.deleteQueue("service-queue");
    }

    private long primeAndGetRequestRTT(SqsRequestor requestor, String queue)
            throws InterruptedException, ExecutionException, TimeoutException {

        final int rttTestCount = 5;

        // Prime the requestor to create the response queue created.
        requestor.request(queue, "rtt", Duration.ofSeconds(30));

        long start = System.currentTimeMillis();
        for (int i = 0; i < rttTestCount; i++) {
            requestor.request(queue, "rtt", Duration.ofSeconds(30));
        }
        long stop = System.currentTimeMillis();

        return (stop - start) / rttTestCount;
    }

    @Test
    public void testRequestReplyScaling() {
        ExecutorService es = Executors.newFixedThreadPool(110);
        /* launch our simple service */
        MyService services[] = new MyService[20];
        for (int i = 0; i < 20; i++) {
            services[i] = new MyService("service-queue-scale", 100);
            es.execute(services[i]);
        }

        // Serially, this would take RTT * 100 requests, so approx 10 seconds
        // plus wire time. Ensure it's less than that.
        try (SqsRequestor requestor = new SqsRequestor("requestor-scaling")) {

            long rtt = primeAndGetRequestRTT(requestor, "service-queue-scale");

            var requests = new ArrayList<CompletableFuture<String>>(100);

            var start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                requests.add(requestor.request("service-queue-scale", "help!"));
            }
            for (CompletableFuture<String> cf : requests) {
                var response = cf.get();
                assertEquals(MyService.RESPONSE_BODY, response);
            }
            var stop = System.currentTimeMillis();

            // a low bar for scaling, but did it go faster than serially?
            long serialDuration = rtt * requests.size();
            long actualDuration = stop - start;
            Assert.assertTrue(String.format("Test did not scale.  Serial duration = %d ms, actual = %d ms.",
                    serialDuration, actualDuration), actualDuration < serialDuration);

            // check for some distribution and that all requests were handled by the service
            int handled = 0;
            for (int i = 0; i < services.length; i++) {
                assertNotEquals(services[i].getReceivedCount(), requests.size());
                handled += services[i].getReceivedCount();
            }

            // some additional for the priming request and RTT testing.
            assertTrue(handled > requests.size());

            Utils.deleteQueue(requestor.getClient(), requestor.getQueueUrl("service-queue-scale"));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown: " + e.getMessage());
        }
    }

    @Test
    public void testRequestReplyMultipleQueues() {
        ExecutorService es = Executors.newFixedThreadPool(10);

        /* launch our simple service */
        MyService service1 = new MyService("service-queue-1", 0);
        MyService service2 = new MyService("service-queue-2", 0);

        es.execute(service1);
        es.execute(service2);

        // Serially, this would take RTT * 100 requests, so approx 10 seconds
        // plus wire time. Ensure it's less than that.
        try (SqsRequestor requestor = new SqsRequestor("multi-queue-requestor")) {

            var requests = new ArrayList<CompletableFuture<String>>(100);
            for (int i = 0; i < 10; i++) {
                requests.add(requestor.request("service-queue-1", "message-" + i));
                requests.add(requestor.request("service-queue-2", "message-" + i));
            }
            for (CompletableFuture<String> cf : requests) {
                cf.get();
            }
            Assert.assertEquals(service1.getReceivedCount(), requests.size() / 2);
            Assert.assertEquals(service2.getReceivedCount(), requests.size() / 2);

            Utils.deleteQueue(requestor.getClient(), requestor.getQueueUrl("service-queue-1"));
            Utils.deleteQueue(requestor.getClient(), requestor.getQueueUrl("service-queue-2"));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown: " + e.getMessage());
        }
    }
}
