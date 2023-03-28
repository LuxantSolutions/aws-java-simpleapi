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

import java.time.Duration;

import com.luxant.sqs.SqsConsumer;

public class SqsReceive {

    public void receiveMessages(String queueName, int count, int seconds) {

        // Creating a consumer is trivial, just create the consumer, and 
        // run it directly or via thread, executor, ect.
        var c = new SqsConsumer(queueName, count, Duration.ofSeconds(seconds), 
            msg -> System.out.printf("Received message: %s\n", msg.body()));     
        c.run();
        
        System.out.printf("\nReceived %d message(s).\n", c.getReceivedCount());
    }

    public static void usage() {
        System.out.println("Usage:  java com.luxant.examples.SqsReceive <queue name> <count> <timeout (secs)>");
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            usage();
            System.exit(1);
        }

        new SqsReceive().receiveMessages(args[0],
               Integer.parseInt(args[1]),
               Integer.parseInt(args[2]));
    }
}
