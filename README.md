# AWS Simple Java API for SNS

This repository contains APIs to work with AWS communication technologies, namely SQS right now.

## Overview

Long story short, I was looking into AWS communication technologies for a project, and found that the SNS API,
while fairly rich, was way more than I needed. So I built simple API for sending, receiving, and creating
and accessing microservices. By no means are the existing APIs lacking, but I just wanted to reduce typing and
support autocreation of queues so all that was required was adding a small amount of application code and
having an API do the heavy lifting. There wasn't a lot of work with sending and receiving, but the services
(request/reply) APIs do a lot of heaving lifting for you. There are existing SQS async and request/reply APIs,
this is just a vastly oversimplified API built to help with a few simple patterns.

This only has whatI require to send and receive messages over SQS utilizing various communications patterns, including microservices. In the future this may be enhanced for FIFO queues, buffering, and performance.

## Usage

There are four primarly classes, [SqsProducer](./src/main/java/com/luxant/sqs/SqsProducer.java),
[SqsConsumer](./src/main/java/com/luxant/sqs/SqsConsumer.java),
[SqsRequestor](./src/main/java/com/luxant/sqs/SqsRequestor.java) and [SqsReplier](./src/main/java/com/luxant/sqs/SqsReplier.java).

### Imports

To use this library you'll want to inclue the following imports:

Producing / Sending messages:

```java
import com.luxant.sqs.SqsProducer;
import com.luxant.sqs.SqsRequestor;
```

Receiving and Handling messages:

```java
import com.luxant.sqs.MessageHandler;
import com.luxant.sqs.SqsConsumer;
import com.luxant.sqs.SqsResponder;
```

### SqsProducer

To send a message just do the following:

```java
        SqsProducer p = new SqsProducer();
        p.sendMessage("my-queue", "hello world!");
```

The SqsProducer will create a client for connecting to AWS, using
your default credentials.

Then, simply send messages. The send operation will create
a queue if necessary, and cache the queue url and send message
request for a bit of performance.

Send as many messages as you would like by just calling the `sendMessage` API.

### SqsConsumer

The consumer implements `Runnable` and is thus designed to run in
a thread or be invoked by an executor.  It accepts an interface that has an 
onMsg method.  This method is invoked for every message received until
the number of messages has been reached.  The timeout specified is an 
internal timeout specfiying how long to block until the next messages
arrives and the next getMessages calls is invoked. By default, 
messages will be deleted after onMsg is called.

```java
        class ConsumerHandler implements Consumer<Message> {
            public String result;

            @Override
            public void acccept(Message m) {
                result = m.body();
                // do some application work here
            }
        }

        // Use in an executor or create a thread to run the consumer.
        ExecutorService exectuor = Executors.newSingleThreadPool();

        // This consumer will exit after 100 messages and internally poll for new
        // messages every 2 seconds.
        SqsConsumer c = new SqsConsumer("my-queue", 100, Duration.ofSeconds(2), new ConsumerHandler());

        // Start consuming messges sent to "my-queue".
        executor.execute(c)
```

The consumer will continue to run until message count is reached, 
the thread is interrupted, or an unrecoverable SQS error occurs.

### SqsServiceResponder

This is the service side of a microservice.  All you need to do is define a consumer
interface and respond via the convenient SqsResponder class.  The SqsResponder
will create an internal response queue a singleton to access that temporary queue will
be used so you can have multiple responders multiplex across this single queue.  Each
JVM instance will have its own internal response queue.

```java
    class MyService implements Consumer<Message>, Runnable {
        SqsResponder responder;

        public MyService(String listenQueue) {
            responder = new SqsResponder(listenQueue, this);
        }

        @Override
        public void accept(Message m) {
            // Do some work here - this is where your application code
            // will process the incoming messages.  Then just use the 
            // convenience API to reply to service requests
            // with the responder. Reply with a String.
            responder.reply(m, "Here's your result.");
        }

        @Override
        public void run() {
            responder.run();
        }
    }
```

The following code starts your service to accept requests: 

```java
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(new MyService("my-queue"));
    // wait for it to complete.
```

Because these are based on queues, you can use the competing consumer pattern to 
scale services. Just launch more instances - that's it.

### SqsServiceRequestor

Now, in order to send messages, you'll just want to make a reqest.  Here we give a geneous 10
seconds to respond.

```java
    String response = requestor.request("my-queue", "Work on this please.", Duration.ofSeconds(10));
```

### Examples

Simple examples of each can be found here:
[SqsReceive.java](./src/main/java/com/luxant/examples/SqsSend.java)
[SqsSend.java](./src/main/java/com/luxant/examples/SqsSend.java)
[SqsServiceRequest.java](./src/main/java/com/luxant/examples/SqsServiceRequest.java)
[SqsServiceResponder.java](./src/main/java/com/luxant/examples/SqsServiceResponder.java)

## Patterns

I've tested these with various patterns which include:

- 1:1 Streaming:  One producer to One consumer
- N:1 Streaming:  Aggregation of data sent by producers
- 1:N Competing Consumer: One producer to many consumers sharing the workload
- 1:1 Microservice: A simple microservice with single requestor
- 1:N Micorservices: Multiple microservice instances load balancing requests

Check out the tests in [SqsPatternsTests.java](./src/test/java/com/luxant/sqs/SqsPatternTests.java).

## Running the Examples

To run the examples, you'll want to build an all in one jar first.

```bash
 $ ./gradlew buildAllInOneJar
 ```

There are a number of scripts in the [scripts](./scripts/) directory.
Run each from the root directory of the project.

### Send to a queue

```bash
$ scripts/sqssend.sh test-queue hello
Sent message to queue test-queue: hello
```

### Receive from a queue

```bash
$ scripts/sqsrecv.sh test-queue 1 2
Received message: hello

Received 1 message(s).
```

### Microservice Requestor and Responder

The example microservice is a small echo service, generally useless
except for demonstrating a test service.

Window 1:

```bash
 $ scripts/sqsresponder.sh my-service
Listening on requests on queue my-service
```

Window 2:

```bash
$ ./scripts/sqsrequest.sh my-service work
Received response: Echo: work
```

Window 1 should print:
`Received request, responding with: Echo: work.`

## Building and Testing

### Requirements

I used the following environment:

```
------------------------------------------------------------
Gradle 7.4.2
------------------------------------------------------------

Build time:   2022-03-31 15:25:29 UTC
Revision:     540473b8118064efcc264694cbcaa4b677f61041

Kotlin:       1.5.31
Groovy:       3.0.9
Ant:          Apache Ant(TM) version 1.10.11 compiled on July 10 2021
JVM:          17.0.6 (Oracle Corporation 17.0.6+9-LTS-190)
OS:           Mac OS X 13.2.1 x86_64

```

Anything older may work, but no guarantees.  Java 8 will not work, 19 does not with Gradle.

### Building

Building alone: `./gradlew build -x test`

For testing, you will need default AWS credentials.

```bash
 tree ~/.aws
/Users/colinsullivan/.aws
├── config
└── credentials
```

You will need to generate an [access key](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html) with permissions to use Amazon services (SQS, etc).  You will want to allow queue creation and deletion.

Build and Test: `./gradlew build`


