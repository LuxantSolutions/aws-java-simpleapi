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

import java.util.HashMap;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

/**
 * Base class for the Simple SQS API package.
 */
public abstract class SqsProvider {

    protected SqsClient sqsClient;
    private HashMap<String, String> queues = new HashMap<>();
    private Object lock = new Object();

    /**
     * Creates a SqsProvider with the provided client.
     * @param c - the SqsClient to use.  If null, a client is created.
     */
    SqsProvider(SqsClient c) {
        sqsClient = (c == null) ? Utils.createClient() : c;
    }

    /**
     * Creates a SqsProvider with a default SqsClient.
     */
    SqsProvider() {
        this(null);
    }

    /**
     * Convenience function to get the SqsClient 
     * @return
     */
    public SqsClient getClient() {
        return sqsClient;
    }

    /**
     * Creates a queue and caches the queue url.
     * @param queueName name of the queue.
     * @return url to cache.
     */
    public String createQueue(String queueName) {
        synchronized(lock) {
            String url = Utils.createQueue(sqsClient, queueName);
            queues.put(queueName, url);
            return url;
        }
    }

    /**
     * Deletes a queue, looking it up from the cached queue
     * urls.
     * @param queueName name of the queue to delete.
     */
    public void deleteQueue(String queueName) {
        synchronized(lock) {
            String url = queues.get(queueName);
            if (url == null) {
                return;
            }
            sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build());
            queues.remove(queueName);
        }
    }

    /**
     * Purges a queue.
     * @param queueName name of the queue
     */
    public void purgeQueue(String queueName) {
        synchronized(lock) {
            String url = queues.get(queueName);
            if (url == null) {
                return;
            }
            sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(url).build());
        } 
    }
    
    /**
     * Cleans up a queue, used with testing and subclasses.
     */
    public void cleanup() {
        synchronized(lock) {
            var urls = queues.keySet();
            for (String u : urls) {
                // TODO: add option to deleteQueue(u);
                purgeQueue(u);
            }
        }
    }
}
