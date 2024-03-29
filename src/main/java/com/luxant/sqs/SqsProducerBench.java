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

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.services.sqs.model.SqsException;

public class SqsProducerBench extends SqsProvider implements Runnable {
    SqsProducer producer;
    String qName;
    int msgRate;
    int msgCount;
    long delay;
    long startTime = 0;
    String body;
    boolean async;

    Logger logger = Logger.getGlobal();

    private static final int NANOSPERSEC = 1000000000;
    private static final int NANOSPERMS = NANOSPERSEC / 1000;

    public void rateLimit(int currentCount) throws InterruptedException {
        if (startTime == 0) {
            startTime = System.nanoTime();
        }
        long elapsed = System.nanoTime() - startTime;
        double r = currentCount / ((double) elapsed / (double) NANOSPERSEC);
        long adj = delay / 20; // 5%
        if (adj == 0) {
            adj = 1; // 1ns min
        }
        if (r < msgRate) {
            delay -= adj;
        } else if (r > msgRate) {
            delay += adj;
        }
        if (delay < 0) {
            delay = 0;
        }

        int nanos;
        long millis = 0;

        if (delay < NANOSPERMS) {
            nanos = (int) delay;
        } else {
            millis = delay / (NANOSPERMS);
            nanos = (int) (delay - (NANOSPERMS * millis));
        }
        Thread.sleep(millis, nanos);
    }    

    private void init(String queueName, String payload, int rate, int count, boolean sendAsync) {    
        qName = queueName;
        body = payload;
        msgRate = rate;
        msgCount = count;
        async = sendAsync;
        producer = new SqsProducer(sqsClient);  
    }

    /**
     * Creates a SqsProducer for use with an executor / threading.
     * A SqsClient will be created.
     * @param queueName - the name of the queue
     * @param rate - the message rate in msgs/sec to send.
     * @param count - the number of messages to send.
     */    
    public SqsProducerBench(String queueName, String payload, int rate, int count, boolean sendAsync) {
        super(null);
        init(queueName, payload, rate, count, sendAsync);
    }

    @Override
    public void run() {
        if (async) {
            runAsync();
        } else {
            runSerially();
        }
    }

    private void sendMessage() {
        producer.sendMessage(qName, body);
        try {
            rateLimit(producer.getSentCount());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sends message serially, one after another.
     */
    public void runSerially() {
        for (int i = 0; i < msgCount && !Thread.currentThread().isInterrupted(); i++) {
            try {
                sendMessage();
            } catch (SqsException e) {
                logger.severe("Exception sending: " + e);
                logger.severe("Exiting producer thread on queue: " + qName);
                return;
            }
        }
        logger.log(Level.FINE,()->"Exiting producer thread on queue: " + qName);
    }

    /**
     * Sends messages asyncronously, constrained by the common thread pool.
     */
    public void runAsync() {
        var ary = new CompletableFuture[msgCount];
        for (int i = 0; i < msgCount && !Thread.currentThread().isInterrupted(); i++) {
            ary[i] = CompletableFuture.runAsync(this::sendMessage);
        }

        try  {
            CompletableFuture.allOf(ary).join();
        } catch (Exception e) {
            logger.severe("Exception sending: " + e);
            logger.severe("Exiting producer thread on queue: " + qName); 
        }
    }

    /**
     * Gets the number of messages sent.
     * @return number of messages sent.
     */
    public int getSentCount() {
        return producer.getSentCount();
    }

    /**
     * Convenience method to get the queue url for this instance.
     * @return queue url
     */
    public String getQueueUrl() {
        return producer.getQueueUrl(qName);
    }
}
