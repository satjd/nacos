package com.alibaba.nacos.consistency.jraft;

import com.alibaba.nacos.consistency.jraft.rpc.DataOperationRequest;
import com.alibaba.nacos.consistency.jraft.rpc.DataOperationResponse;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

/**
 * @author satjd
 */
public class DataOperationClosure implements Closure {

    private RaftConsistencyServer raftConsistencyServer;
    private DataOperationRequest request;
    private DataOperationResponse response;
    private Closure done;

    public DataOperationClosure(RaftConsistencyServer raftConsistencyServer, DataOperationRequest request, DataOperationResponse response, Closure done) {
        this.raftConsistencyServer = raftConsistencyServer;
        this.request = request;
        this.response = response;
        this.done = done;
    }

    @Override
    public void run(Status status) {
        if (this.done != null) {
            done.run(status);
        }
    }

    public DataOperationRequest getRequest() {
        return request;
    }

    public DataOperationResponse getResponse() {
        return response;
    }
}
