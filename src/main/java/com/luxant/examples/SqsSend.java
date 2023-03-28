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

import com.luxant.sqs.SqsProducer;

public class SqsSend {
    public void sendMessage(String queueName, String message) {
        SqsProducer p = new SqsProducer();
        p.sendMessage(queueName, message);
        System.out.printf("Sent message to queue %s: %s\n", queueName, message);
    }

    public static void usage() {
        System.out.println("Usage:  java com.luxant.examples.SqsSend <queue name> <payload> ");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        new SqsSend().sendMessage(args[0], args[1]);
    }
}
