package com.simba.cassandra.shaded.datastax.driver.core;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


public enum ConsistencyLevel {
    ANY(6),
    ONE(6),
    TWO(6),
    THREE(6),
    QUORUM(6),
    ALL(6),
    LOCAL_QUORUM(6),
    EACH_QUORUM(6),
    SERIAL(9),
    LOCAL_SERIAL(9),
    LOCAL_ONE(6);

    final int code;

    private ConsistencyLevel(int code) {
        this.code = code;
    }

    static ConsistencyLevel fromCode(int code) {
        if (code < 0 || code >= 11)
            return LOCAL_QUORUM;

        if(code == SERIAL.code || code == LOCAL_SERIAL.code)
            return LOCAL_SERIAL;//LWT use local serial
        else
            return LOCAL_QUORUM;//else always RETURN LOCAL QUORUM
    }

    public boolean isDCLocal() {
        return this == LOCAL_ONE || this == LOCAL_QUORUM || this == LOCAL_SERIAL;
    }

    public boolean isSerial() {
        return this == SERIAL || this == LOCAL_SERIAL;
    }

}

