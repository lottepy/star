package com.magnumresearch.aqumon.riskengine.constants;

public enum DataSourceType {
    //Datamaster
    DATAMASTER(0),
    //Snapshot database
    SNAPSHOT(1),
    //Current Database
    DATABASE(2),
    //History Database
    HISTORYDATABASE(3),
    //External Broker
    BROKER(4),
    //Pre Defined Value
    DEFAULT(5);

    private final int type;

    DataSourceType(int type) {
        this.type = type;
    }
}