package com.alibaba.nacos.consistency.jraft;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

/**
 * @author satjd
 */
public class RaftConsistencyClient {
    private final Logger LOG = LoggerFactory.getLogger(RaftConsistencyClient.class);

    private final BoltCliClientService cliClientService;
    private final String groupId;

    public RaftConsistencyClient(String groupId, String confStr)
        throws TimeoutException, InterruptedException {
        this.groupId = groupId;
        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        cliClientService = new BoltCliClientService();
        cliClientService.init(new CliOptions());
    }

    public Object invokeSync(Serializable serializable)
        throws TimeoutException, InterruptedException, RemotingException {
        int timout = 1000;
        final PeerId leader;

        RouteTable.getInstance().refreshConfiguration(cliClientService, groupId, timout);
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, timout)
            .isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        } else {
            leader = RouteTable.getInstance().selectLeader(groupId);
        }

        return cliClientService.getRpcClient().invokeSync(leader.getEndpoint().toString(),
            serializable, 1000);
    }

    public String getLeader() {
        PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        if (leader == null) {
            return "null";
        }

        return leader.getEndpoint().toString();
    }
}
