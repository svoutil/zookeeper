package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.QuorumBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

/**
 * Created by svoutilainen on 12/23/14.
 */
public class MissingTxnLogCorruptionTest extends QuorumBase {
    private static final Logger LOG = LoggerFactory.getLogger(MissingTxnLogCorruptionTest.class);

    @Before
    public void setUp() throws Exception {
        super.setUp(false /* no observers */);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void startServer(int index) throws IOException, InterruptedException {
        setupServer(index + 1); // sids are base 1...
        getPeerList().get(index).start();
        waitForServerUp(index);
    }

    private void waitForServerUp(int index) throws InterruptedException {
        LOG.info("Waiting for server up for sid {}", index + 1);
        Assert.assertTrue(waitForServerUp("127.0.0.1:" + getClientPortByIndex(index), 30000));
    }

    private void waitForLeaderChange(int currentLeader, long timeout)
            throws InterruptedException, TimeoutException {

        long timeWaited = 0;

        while (true) {
            int leaderIndex = getLeaderIndex();

            if (leaderIndex != -1 && getLeaderIndex() + 1 != currentLeader) {
                LOG.info("Leader changed from {} to {}", currentLeader, getLeaderIndex() + 1);
                return;
            }

            if (timeWaited >= timeout) {
                throw new TimeoutException();
            }

            long timeToWait = Math.min(500, timeout - timeWaited);

            Thread.sleep(timeToWait);

            timeWaited += timeToWait;
        }
    }

    @Test
    public void txnLogGapAfterSyncViaSnap() throws Exception {
        final byte[] data = "Hint Water".getBytes();

        ArrayList<QuorumPeer> peers;
        int leaderIndex = getLeaderIndex();
        int gapTargetPeerIndex;

        for (int i = 0; i < 5; i ++) {
            waitForServerUp(i);
        }


        // If leader is host with highest sid
        //      close it
        //      bring it back
        //      assert it is no longer the leader
        if (leaderIndex == 4) {
            LOG.info("Sid 5 is the leader, moving the leader");
            QuorumBase.shutdown(getPeerList().get(leaderIndex));
            waitForLeaderChange(leaderIndex + 1, 30000);
            startServer(leaderIndex);
            leaderIndex = getLeaderIndex();
            Assert.assertTrue("Leader did not change from sid 5", leaderIndex != 4);
            LOG.info("New leader is Sid {}", leaderIndex + 1);
        }

        ZooKeeper client = createClient();

        // Insert some initial data
        LOG.info("Inserting some initial data");
        client.create("/foo", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo1", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo2", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo3", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.close();

        peers = getPeerList();
        leaderIndex = getLeaderIndex();
        gapTargetPeerIndex = leaderIndex != 3 ? 3 : 2;

        // Close two hosts including the one with the highest sid but not the leader
        LOG.info("Shutting down sids 5 and {}", gapTargetPeerIndex + 1);
        QuorumBase.shutdown(peers.get(4));
        QuorumBase.shutdown(peers.get(gapTargetPeerIndex));

        // Insert some more data
        LOG.info("Inserting some more data with two nodes down");
        client = createClient();
        client.create("/foo4", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo5", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo6", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create("/foo7", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.close();

        // Turn on force snap
        LOG.info("Enable forcing a snap");
        LearnerHandler.forceSnapSync = true;

        // Bring up host with highest sid
        LOG.info("Bringing up sid 5");
        startServer(4);

        peers = getPeerList();

        // Validate it gets a snap
        Assert.assertEquals("Sid 5 should have been sync'd with a snap",
                Leader.SNAP,
                peers.get(4).syncWithLeaderType);

        // Close the leader
        LOG.info("Shutdown the leader, sid {}", leaderIndex + 1);
        QuorumBase.shutdown(peers.get(leaderIndex));
        waitForLeaderChange(leaderIndex + 1, 30000);

        LOG.info("Restarting previous leader, sid {}", leaderIndex + 1);
        startServer(leaderIndex);

        // Assert that the highest sid became the leader
        Assert.assertEquals("Sid 5 should have become the leader",
                4,
                getLeaderIndex());

        leaderIndex = 4;

        // Disable force snap and ensure that next sync will be a DIFF
        LOG.info("Reenable and force snap");
        LearnerHandler.forceSnapSync = false;
        peers.get(leaderIndex).leader.zk.getZKDatabase().setSnapshotSizeFactor(1000);

        // Bring back other node taken down earlier
        LOG.info("Bring back gap target host, sid {}", gapTargetPeerIndex + 1);
        startServer(gapTargetPeerIndex);

        peers = getPeerList();

        // Assert that it is caught up with a DIFF
        Assert.assertEquals("Sid " + gapTargetPeerIndex + " should have been sync'd with a diff",
                Leader.DIFF,
                peers.get(gapTargetPeerIndex).syncWithLeaderType);

        // Check for existence of nodes created while both hosts were down
        LOG.info("Creating client connected to leader to check for corruption");
        ZooKeeper leaderClient = createClient("127.0.0.1:" + getClientPortByIndex(leaderIndex));
        LOG.info("Creating client connected to gap target to check for corruption");
        ZooKeeper gapTargetClient = createClient("127.0.0.1:" + getClientPortByIndex(gapTargetPeerIndex));

        Stat leaderStat;
        Stat gapTargetStat;

        LOG.info("Check status for node /foo4");
        leaderStat = leaderClient.exists("/foo4", false);
        gapTargetStat = gapTargetClient.exists("/foo4", false);

        // Assert they do not exist on last node brought up but do exist on other nodes
        Assert.assertNotNull("Node should exist on leader but it does NOT", leaderStat);
        Assert.assertNotNull("Node should also exist on target if there is no corruption but it does NOT", gapTargetStat);
    }
}
