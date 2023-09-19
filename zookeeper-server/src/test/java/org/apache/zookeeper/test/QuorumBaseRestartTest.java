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

package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QuorumBaseRestartTest extends QuorumBase {

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp(true, true);
    }

    @Test
    public void testLeaderRestart() throws Exception {
        int leaderIndex = getLeaderIndex();
        int leaderPort = getLeaderClientPort();
        ZooKeeper zk = createClient();
        zk.create("/a1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/a2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/a3", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        shutdown(getLeaderQuorumPeer());
        setupServer(leaderIndex + 1);
        QuorumPeer quorumPeer = getPeerList().get(leaderIndex);
        quorumPeer.start();
        createClient("127.0.0.1:" + leaderPort, 2 * CONNECTION_TIMEOUT);
    }
}
