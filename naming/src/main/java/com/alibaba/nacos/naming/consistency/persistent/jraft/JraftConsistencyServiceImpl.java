package com.alibaba.nacos.naming.consistency.persistent.jraft;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.consistency.jraft.RaftConsistencyClient;
import com.alibaba.nacos.consistency.jraft.RaftConsistencyServer;
import com.alibaba.nacos.consistency.jraft.rpc.DataOperationRequest;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.PersistentConsistencyService;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Record;
import com.alipay.remoting.exception.RemotingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * @author satjd
 */

@Service
public class JraftConsistencyServiceImpl implements PersistentConsistencyService {
    private ObjectMapper objectMapper = new ObjectMapper();

    private RaftConsistencyServer server;
    private RaftConsistencyClient client;

    private final String dataPath = UtilsAndCommons.DATA_BASE_DIR + File.separator + "jraft_data";

    private final String groupId = "naming-jraft";

    private final int JRAFT_SERVER_PORT = 8899;

    private Map<RecordListener, DataObserverProxy> proxyMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() throws Exception {
        String initConf = makeJraftServerInitConf();
        server = RaftConsistencyServer.getRaftConsistencyServer(dataPath,groupId
            ,makeJraftLocalServerId()
            ,initConf);
        Loggers.RAFT.info("[JRAFT] Jraft server run on port:{}",JRAFT_SERVER_PORT);

        client = new RaftConsistencyClient(groupId,initConf);
    }

    private String makeJraftLocalServerId() {
        return NetUtils.getLocalAddress()
            + UtilsAndCommons.IP_PORT_SPLITER
            + JRAFT_SERVER_PORT;
    }
    private String makeJraftServerInitConf() {
        // read from cluster.conf
        StringBuilder sb = new StringBuilder();

        // replace nacos server port into jraft rpc port
        try {
            for (String s : SystemUtils.readClusterConf()) {
                if (s.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
                    String temp = s.substring(0,s.indexOf(UtilsAndCommons.IP_PORT_SPLITER));
                    temp += UtilsAndCommons.IP_PORT_SPLITER;
                    temp += JRAFT_SERVER_PORT;

                    sb.append(temp).append(",");
                }
            }
        } catch (IOException e) {
            Loggers.RAFT.error("[JRAFT] Cannot Read cluster.conf Exception:{}",e.toString());
        }

        return sb.toString();
    }

    public String getLeaderAddress() throws TimeoutException, InterruptedException {
        return client.getLeader();
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        DataOperationRequest request = new DataOperationRequest();
        request.setKey(key);
        request.setOperationType("UPDATE");
        // Record -> json
        request.setValue(JSON.toJSONString(value));

        try {
            client.invokeSync(request);
        } catch (TimeoutException e) {
            Loggers.RAFT.error("jraft cli invoke timeout. key={}",key);
        } catch (InterruptedException e) {
            Loggers.RAFT.error("jraft cli invoke interrupted. key={}",key);
        } catch (RemotingException e) {
            Loggers.RAFT.error("jraft cli invoke raise remoting error. key={}",key);
        }
    }

    @Override
    public void remove(String key) throws NacosException {
        DataOperationRequest request = new DataOperationRequest();
        request.setKey(key);
        request.setOperationType("DELETE");

        // Record -> json
        request.setValue("DELETE");

        try {
            client.invokeSync(request);
        } catch (TimeoutException e) {
            Loggers.RAFT.error("jraft cli invoke timeout. key={}",key);
        } catch (InterruptedException e) {
            Loggers.RAFT.error("jraft cli invoke interrupted. key={}",key);
        } catch (RemotingException e) {
            Loggers.RAFT.error("jraft cli invoke raise remoting error. key={}",key);
        }
    }

    @Override
    public Datum get(String key) throws NacosException {
        return null;
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        proxyMap.putIfAbsent(listener,new DataObserverProxy(listener));
        server.registerDataObserver(key, proxyMap.get(listener));
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        DataObserverProxy proxyToRemove = proxyMap.get(listener);
        server.unregisterDataObserver(key,proxyToRemove);
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
