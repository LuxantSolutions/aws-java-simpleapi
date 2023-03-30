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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.luxant.sqs.SqsResponder;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsServiceResponder {

    ExecutorService executor = Executors.newSingleThreadExecutor();

    class EchoService implements Consumer<Message>, Runnable {
        SqsResponder responder;

        public EchoService(String listenQueue) {
            responder = new SqsResponder(listenQueue, this);
        }

        @Override
        public void accept(Message m) {
            String respMsg = String.format("Echo: %s", m.body());
            System.out.printf("Received request, responding with: %s.\n", respMsg);
            responder.reply(m, respMsg);
        }

        @Override
        public void run() {
            responder.run();
        }
    }

    public Future<Void> startResponding(String queueName) {
        try {
            return executor.submit(new EchoService(queueName), null);
        } catch (Exception e) {
            System.out.println(e);
            throw e;
        }
    }

    public static void usage() {
        System.out.println("Usage:  java com.luxant.examples.SqsServiceResponder <queue name>");
    }

    // For tests...
    public void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            usage();
            System.exit(1);
        }

        try {
            var f  = new SqsServiceResponder().startResponding(args[0]);
            System.out.printf("Listening on requests on queue %s\n", args[0]);
            f.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
