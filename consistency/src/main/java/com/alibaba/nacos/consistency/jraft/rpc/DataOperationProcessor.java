package com.alibaba.nacos.consistency.jraft.rpc;

import com.alibaba.nacos.consistency.jraft.DataOperationClosure;
import com.alibaba.nacos.consistency.jraft.RaftConsistencyServer;
import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author satjd
 */
public class DataOperationProcessor extends AsyncUserProcessor<DataOperationRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(DataOperationProcessor.class);

    private final RaftConsistencyServer raftConsistencyServer;

    public DataOperationProcessor(RaftConsistencyServer raftConsistencyServer) {
        this.raftConsistencyServer = raftConsistencyServer;
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final DataOperationRequest request) {
        if (!this.raftConsistencyServer.getFsm().isLeader()) {
            asyncCtx.sendResponse(this.raftConsistencyServer.redirect());
            return;
        }

        final DataOperationResponse response = new DataOperationResponse();
        final DataOperationClosure closure = new DataOperationClosure(raftConsistencyServer, request, response,
            status -> {
                if (!status.isOk()) {
                    response.setErrorMsg(status.getErrorMsg());
                    response.setSuccess(false);
                }
                asyncCtx.sendResponse(response);
            });

        try {
            final Task task = new Task();
            task.setDone(closure);
            task.setData(ByteBuffer
                .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));

            // apply task to raft group.
            raftConsistencyServer.getNode().apply(task);
            LOG.info("apply operation: key= " + request.getKey());
        } catch (final CodecException e) {
            LOG.error("Fail to encode DataOperationRequest", e);
            response.setSuccess(false);
            response.setErrorMsg(e.getMessage());
            asyncCtx.sendResponse(response);
        }
    }

    @Override
    public String interest() {
        return DataOperationRequest.class.getName();
    }
}
