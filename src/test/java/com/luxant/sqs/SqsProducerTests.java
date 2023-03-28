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
