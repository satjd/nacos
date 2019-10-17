package com.alibaba.nacos.consistency.jraft.rpc;

import java.io.Serializable;

/**
 * @author satjd
 */
public class DataOperationRequest implements Serializable {

    private static final long serialVersionUID = -5593506698946830831L;

    private String operationType;

    private String key;

    private String value;

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
