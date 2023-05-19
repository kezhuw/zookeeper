/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ClientCnxnSocketNettyTest {
    private static class BusyServer implements AutoCloseable {
        private final ServerSocket server;
        private final Socket client;

        public BusyServer() throws IOException {
            // Server with one backlog.
            this.server = new ServerSocket(0, 1);
            // Occupy that backlog.
            this.client = new Socket("127.0.0.1", server.getLocalPort());
        }

        public int getLocalPort() {
            return server.getLocalPort();
        }
        @Override
        public void close() throws Exception {
            client.close();
            server.close();
        }
    }

    @Test
    public void testDanglingConnectFuture() throws Exception {
        ServerSocket idleServer = new ServerSocket(0);
        BusyServer busyServer = new BusyServer();

        LinkedBlockingDeque<ClientCnxn.Packet> ongoingQueue = new LinkedBlockingDeque<>();

        ClientCnxnSocketNetty netty = Mockito.spy(new ClientCnxnSocketNetty(new ZKClientConfig()));
        AtomicReference<Duration> sleepBeforeClose = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            Duration duration = sleepBeforeClose.get();
            if (duration == null) {
                return null;
            }
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException ignored) {}
            return null;
        }).when(netty).beforeChannelClose();

        // Let's ignore NPE on SendThread for demonstration test.
        netty.introduce(null, 0, ongoingQueue);

        netty.connect(InetSocketAddress.createUnresolved("127.0.0.1", busyServer.getLocalPort()), 400);
        assertTrue(netty.isConnected());
        // Connect is asynchronous, sleep a while to let it going into connecting stage.
        Thread.sleep(100);
        netty.cleanup();
        assertFalse(netty.isConnected());

        Assumptions.assumeTrue(ongoingQueue.isEmpty());

        netty.connect(new InetSocketAddress("127.0.0.1", idleServer.getLocalPort()));
        assertNotNull(ongoingQueue.take());

        sleepBeforeClose.set(Duration.ofMillis(800));
        netty.cleanup();

        netty.close();
        idleServer.close();
        busyServer.close();
    }
}
