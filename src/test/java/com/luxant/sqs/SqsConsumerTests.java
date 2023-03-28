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
