package com.alibaba.nacos.consistency.jraft.snapshot;

import com.alibaba.nacos.consistency.jraft.BizDomain;

/**
 * 将通过snapshot load时，将业务状态装载进来的接口
 * @author satjd
 */
@FunctionalInterface
public interface SnapshotLoadOp {
    void onSnapshotDataLoad(BizDomain domainObj);
}
