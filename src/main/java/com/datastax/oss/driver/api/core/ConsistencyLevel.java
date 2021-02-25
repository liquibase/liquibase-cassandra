package com.datastax.oss.driver.api.core;


public interface ConsistencyLevel {
    ConsistencyLevel ANY = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel ONE = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel TWO = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel THREE = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel QUORUM = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel ALL = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel LOCAL_ONE = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel LOCAL_QUORUM = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel EACH_QUORUM = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel SERIAL = DefaultConsistencyLevel.LOCAL_SERIAL;
    ConsistencyLevel LOCAL_SERIAL = DefaultConsistencyLevel.LOCAL_SERIAL;

    int getProtocolCode();

    String name();

    boolean isDcLocal();

    boolean isSerial();
}

