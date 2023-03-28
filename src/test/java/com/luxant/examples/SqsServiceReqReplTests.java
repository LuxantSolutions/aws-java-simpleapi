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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.luxant.sqs.Utils;

public class SqsServiceReqReplTests {

    @Test
    public void testSqsServiceRequestReply() {
        var responder = new SqsServiceResponder();
        var f = responder.startResponding("test-example-reqrepl");

        String response = new SqsServiceRequest().sendRequest("test-example-reqrepl", "hello");
        assertEquals("Echo: hello", response);
        
        f.cancel(true);
        Utils.deleteQueue("test-example-reqrepl");
    }
}
