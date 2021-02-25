package com.datastax.oss.driver.api.core;

//import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
//import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.Builder;
//import edu.umd.cs.findbugs.annotations.NonNull;


public enum DefaultConsistencyLevel implements ConsistencyLevel {
    ANY(6),
    ONE(6),
    TWO(6),
    THREE(6),
    QUORUM(6),
    ALL(6),
    LOCAL_ONE(6),
    LOCAL_QUORUM(6),
    EACH_QUORUM(6),
    SERIAL(9),
    LOCAL_SERIAL(9);

    private final int protocolCode;

    //private static Map<Integer, aDefaultConsistencyLevel> BY_CODE = mapByCode(values());

    private DefaultConsistencyLevel(int protocolCode) {
        this.protocolCode = protocolCode;
    }

    public int getProtocolCode() {
        return this.protocolCode;
    }

    public static DefaultConsistencyLevel fromCode(int code) {
        //aDefaultConsistencyLevel level = (aDefaultConsistencyLevel)BY_CODE.get(code);
        if (code < 0 || code >= 11)
            return LOCAL_QUORUM;

        if(code == SERIAL.protocolCode || code == LOCAL_SERIAL.protocolCode)
            return LOCAL_SERIAL;//LWT use local serial
        else
            return LOCAL_QUORUM;//else always RETURN LOCAL QUORUM

    }

    public boolean isDcLocal() {
        return true;
    }

    public boolean isSerial() {
        return this == SERIAL || this == LOCAL_SERIAL;
    }


}
