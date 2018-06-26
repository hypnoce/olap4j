/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.driver.xmla.proxy;

import junit.framework.TestCase;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;

import static org.mockito.Mockito.never;

public class ArcClientTest extends TestCase {

    private Client client;
    private ArcClient arcClient;

    public ArcClientTest() {
        super();
        client = Mockito.mock(Client.class);
        arcClient = new ArcClient(client);
    }

    public void test_releasing_the_arc_client_should_return_true_when_it_is_the_last_release() {
        assertTrue(arcClient.release());
    }

    public void test_releasing_the_arc_client_should_return_false_when_it_is_not_the_last_release() {
        arcClient.retain();
        assertFalse(arcClient.release());
    }

    public void test_using_the_arc_client_once_and_releasing_should_close_the_client() {
        arcClient.release();
        Mockito.verify(client).close();
    }

    public void test_releasing_the_arc_client_should_only_close_the_client_when_all_retains_have_been_released() {
        arcClient.retain();
        Mockito.verify(client, never()).close();

        arcClient.release();
        Mockito.verify(client, never()).close();

        arcClient.release();
        Mockito.verify(client).close();
    }
}
