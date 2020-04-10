package com.alibaba.nacos.core.pushv2;

@FunctionalInterface
public interface CallBack {
    void onCallBack(PushDataObject pushDataObject);
}
