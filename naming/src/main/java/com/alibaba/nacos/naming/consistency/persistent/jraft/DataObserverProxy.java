package com.alibaba.nacos.naming.consistency.persistent.jraft;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.consistency.jraft.BizDomain;
import com.alibaba.nacos.consistency.jraft.DataObserver;
import com.alibaba.nacos.consistency.jraft.rpc.DataOperationRequest;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author satjd
 */
public class DataObserverProxy implements DataObserver {
    private static ObjectMapper objectMapper = new ObjectMapper();

    private RecordListener recordListener;

    public DataObserverProxy(RecordListener recordListener) {
        this.recordListener = recordListener;
    }

    @Override
    public BizDomain applyOperation(DataOperationRequest operation) {
        try {
            switch (operation.getOperationType()) {
                case "ADD" :
                case "UPDATE" :
                    if (KeyBuilder.matchSwitchKey(operation.getKey())) {
                        SwitchDomain switchDomain = JSON.parseObject(operation.getValue(), SwitchDomain.class);
                        recordListener.onChange(operation.getKey(),switchDomain);
                    } else if (KeyBuilder.matchInstanceListKey(operation.getKey())) {
                        Instances instances = JSON.parseObject(operation.getValue(), Instances.class);
                        recordListener.onChange(operation.getKey(),instances);
                    } else if (KeyBuilder.matchServiceMetaKey(operation.getKey())) {
                        Service service = JSON.parseObject(operation.getValue(), Service.class);
                        recordListener.onChange(operation.getKey(),service);
                    }
                    // todo change biz domain
                    break;
                case "DELETE" :
                    // todo change biz domain
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            Loggers.RAFT.error("[JRAFT] exception on apply operation.key={}, exception:{}",operation.getKey(),e);
        }

        // todo retrieve biz domain data
        BizDomain ret = new BizDomain();
        ret.domainObj = "";
        ret.domainId = "SWITCH";
        return ret;
    }
}
