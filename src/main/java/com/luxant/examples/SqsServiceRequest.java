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

import com.luxant.sqs.SqsRequestor;

public class SqsServiceRequest {

    public String sendRequest(String queueName, String message) {
        String response = null;
        try (var r = new SqsRequestor("example-requestor")) {
            response = r.request(queueName, message, Duration.ofSeconds(20));
            System.out.printf("Received response: %s\n", response);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
        return response;
    }

    public static void usage() {
        System.out.println("Usage: java com.luxant.examples.SqsServiceRequest <queue name> <payload>");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        new SqsServiceRequest().sendRequest(args[0], args[1]);
    }
}
