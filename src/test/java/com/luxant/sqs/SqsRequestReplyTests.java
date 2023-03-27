package com.luxant.sqs;

import org.junit.Test;

import software.amazon.awssdk.services.sqs.model.Message;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class SqsRequestReplyTests {

    private class testService implements MessageHandler, Runnable {
        static final public String RESPONSE_BODY = "Here's some help.";
        SqsResponder responder;

        public testService(String listenQueue, int workDelay) {
            responder = new SqsResponder(listenQueue, this);
        }

        @Override
        public void onMsg(Message m) {
            responder.reply(m, RESPONSE_BODY);
        }

        @Override
        public void run() {
            responder.run();
        }
    }

    @Test
    public void testRequestReplySimple() {
        ExecutorService es = Executors.newFixedThreadPool(10);
        es.execute(new testService("test-reqrep-simple", 1000));

        try (SqsRequestor requestor = new SqsRequestor("requestor")) {
            String response = requestor.request("test-reqrep-simple", "help!", Duration.ofSeconds(10));
            assertEquals(testService.RESPONSE_BODY, response);
            Utils.deleteQueue(requestor.getClient(), requestor.getQueueUrl("test-reqrep-simple"));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown: " + e.getMessage());
        }
    }
}
