package com.luxant.sqs;

import software.amazon.awssdk.services.sqs.model.Message;

public interface MessageHandler {
    public void onMsg(Message m);
}