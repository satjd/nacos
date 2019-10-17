package com.alibaba.nacos.consistency.jraft.rpc;

import java.io.Serializable;

public class DataOperationResponse implements Serializable {
    private static final long serialVersionUID = -5798361749293412747L;

    private String result;
    private boolean success;

    private String redirect;

    private String errorMsg;

    public DataOperationResponse(String result, boolean success, String redirect, String errorMsg) {
        super();
        this.result = result;
        this.success = success;
        this.redirect = redirect;
        this.errorMsg = errorMsg;
    }

    public DataOperationResponse() {
        super();
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getRedirect() {
        return redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return "DataOperationResponse{" +
            "value='" + result + '\'' +
            ", success=" + success +
            ", redirect='" + redirect + '\'' +
            ", errorMsg='" + errorMsg + '\'' +
            '}';
    }
}
