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
