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

        // receive at least one message.
        var msgs = new ArrayList<Message>();
        while (msgs.size() < 100) {
            msgs.addAll(Utils.receiveMessages(client, qUrl, 2));
        }

        assertEquals(100, msgs.size());
        assertEquals("hello", msgs.get(0).body());

        Utils.deleteQueue(p.getClient(), qUrl);
    }
}
