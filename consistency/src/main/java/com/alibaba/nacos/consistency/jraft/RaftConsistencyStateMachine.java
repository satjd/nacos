/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.consistency.jraft;

import com.alibaba.nacos.consistency.jraft.rpc.DataOperationRequest;
import com.alibaba.nacos.consistency.jraft.snapshot.OperationSnapshotFile;
import com.alibaba.nacos.consistency.jraft.snapshot.SnapshotLoadOp;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author satjd
 */
public class RaftConsistencyStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsistencyStateMachine.class);

    private final ConcurrentHashMap<String, BizDomain> bizDomains = new ConcurrentHashMap<>();

    /** todo 装载数据 */
    private final SnapshotLoadOp snapshotLoadOp = domainObj -> {};

    private final ConcurrentHashMap<String, List<DataObserver>> observers = new ConcurrentHashMap<>();

    // private final
    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            String key = "";
            DataOperationRequest request = null;

            DataOperationClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (DataOperationClosure) iter.done();
                key = closure.getRequest().getKey();
                request = closure.getRequest();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    request = SerializerManager.getSerializer(SerializerManager.Hessian2)
                        .deserialize(data.array(), DataOperationRequest.class.getName());
                    key = request.getKey();
                } catch (final CodecException e) {
                    LOG.error("Fail to decode", e);
                }
            }

            List<DataObserver> observersToNotify = observers.get(key);
            if (observersToNotify != null) {
                for (DataObserver dataObserver : observersToNotify) {
                    BizDomain bizDomain = dataObserver.applyOperation(request);
                    bizDomains.put(bizDomain.domainId,bizDomain);
                }
            }

            if (closure != null) {
                closure.getResponse().setResult(request.getKey());
                closure.getResponse().setSuccess(true);
                closure.run(Status.OK());
            }
            LOG.info("log (key={})added at logIndex={}", key, iter.getIndex());
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        Utils.runInThread(() -> {
            final OperationSnapshotFile snapshot = new OperationSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(bizDomains)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save vipserver snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: %s", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final OperationSnapshotFile snapshot = new OperationSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.bizDomains.putAll(snapshot.load());
            // reload all business domain's data
            for (Map.Entry<String,BizDomain> e : bizDomains.entrySet()) {
                snapshotLoadOp.onSnapshotDataLoad(e.getValue());
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}
