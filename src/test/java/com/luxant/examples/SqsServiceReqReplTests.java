package com.luxant.examples;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.luxant.sqs.Utils;

public class SqsServiceReqReplTests {

    @Test
    public void testSqsServiceRequestReply() {
        var responder = new SqsServiceResponder();
        var f = responder.startResponding("test-example-reqrepl");

        String response = new SqsServiceRequest().sendRequest("test-example-reqrepl", "hello");
        assertEquals("Echo: hello", response);
        
        f.cancel(true);
        Utils.deleteQueue("test-example-reqrepl");
    }
}
