package com.alibaba.nacos.consistency.jraft;

import com.alibaba.nacos.consistency.jraft.rpc.DataOperationRequest;

/**
 * @author satjd
 */
public interface DataObserver {

    /** 具体的apply日志到状态机的操作
     * @param operation
     * @return 返回系统需要达到的一致性状态域
     */
    public BizDomain applyOperation(DataOperationRequest operation);
}
