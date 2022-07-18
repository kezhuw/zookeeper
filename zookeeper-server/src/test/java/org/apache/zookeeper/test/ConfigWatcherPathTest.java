package org.apache.zookeeper.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

public class ConfigWatcherPathTest extends ClientBase {
    private void testGetConfigWatcherPathWithChroot(String chroot) throws Exception {
        ZooKeeper zk1 = createClient(hostPort + chroot);
        BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        byte[] config = zk1.getConfig(events::add, null);

        ZooKeeper zk2 = createClient();
        zk2.addAuthInfo("digest", "super:test".getBytes());
        zk2.setData(ZooDefs.CONFIG_NODE, config, -1);

        WatchedEvent event = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event, "no event after 10s");
        assertEquals(Watcher.Event.KeeperState.SyncConnected, event.getState());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, event.getType());
        assertEquals(ZooDefs.CONFIG_NODE, event.getPath());
    }

    @Test
    public void testGetConfigWatcherPathWithShortChroot() throws Exception {
        testGetConfigWatcherPathWithChroot("/short");
    }

    @Test
    public void testGetConfigWatcherPathWithLongChroot() throws Exception {
        testGetConfigWatcherPathWithChroot("/pretty-long-chroot-path");
    }

    /**
     * This test will fail due to abnormal event path.
     */
    @Test
    public void testGetConfigWatcherPathWithChrootZoo() throws Exception {
        testGetConfigWatcherPathWithChroot("/zoo");
    }

    /**
     * This test will fail due to no matching watcher in client for stripped client path.
     *
     * {@link ZooKeeper#getConfig(Watcher, Stat)} registers watcher using path "/zookeeper/config".
     * With chroot "/zookeeper", notifications to "/zookeeper/config" will be stripped to path "/config"
     * in {@link org.apache.zookeeper.ClientCnxn.SendThread#stripChroot(String)}. Finally, the watcher
     * gets no notifications.
     *
     * <p>Turn {@link ZooKeeper#WATCH_ZOOKEEPER_CONFIG_USING_CLIENT_PATH} to true to fix above problem.</p>
     */
    @Test
    public void testGetConfigWatcherPathWithChrootZooKeeper() throws Exception {
        ZooKeeper.WATCH_ZOOKEEPER_CONFIG_USING_CLIENT_PATH = false;

        testGetConfigWatcherPathWithChroot("/zookeeper");
    }

    /**
     * See above comments.
     */
    @Test
    public void testGetConfigWatcherPathWithChrootZooKeeperConfig() throws Exception {
        ZooKeeper.WATCH_ZOOKEEPER_CONFIG_USING_CLIENT_PATH = false;

        testGetConfigWatcherPathWithChroot("/zookeeper/config");
    }
}
