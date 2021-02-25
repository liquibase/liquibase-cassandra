package com.datastax.driver.core;

/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/***
 * Modified to provide consistency level backwards compatibility
 */
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

    // Used by the native protocol
    final int code;

    //private static final ConsistencyLevel[] codeIdx;

    static {
        //int maxCode = -1;
        //for (ConsistencyLevel cl : com.datastax.driver.core.ConsistencyLevel.values()) maxCode = Math.max(maxCode, cl.code);
        //codeIdx = new ConsistencyLevel[maxCode + 1];
        /*
        for (ConsistencyLevel cl : com.datastax.driver.core.ConsistencyLevel.values()) {
            if (codeIdx[cl.code] != null) throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }*/
    }



    private ConsistencyLevel(int code) {
        if(code == 8 || code == 9)//SERIAL or LOCAL SERIAL than LOCAL SERIAL
            this.code = 9;
        else
            this.code = 6;//ELSE LOCAL_QUORUM
    }


    static ConsistencyLevel fromCode(int code) {
        if (code < 0 || code >= 11)
            throw new RuntimeException(String.format("Unknown code %d for a consistency level", code));

        if(code == SERIAL.code || code == LOCAL_SERIAL.code)
            return LOCAL_SERIAL;//LWT use local serial
        else
            return LOCAL_QUORUM;//else always RETURN LOCAL QUORUM
    }

    /**
     * Whether or not this consistency level applies to the local data-center only.
     *
     * @return whether this consistency level is {@code LOCAL_ONE}, {@code LOCAL_QUORUM}, or {@code
     *     LOCAL_SERIAL}.
     */
    public boolean isDCLocal() {
        return true;
    }

    /**
     * Whether or not this consistency level is serial, that is, applies only to the "paxos" phase of
     * a <a
     * href="https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_ltwt_transaction_c.html">Lightweight
     * transaction</a>.
     *
     * <p>Serial consistency levels are only meaningful when executing conditional updates ({@code
     * INSERT}, {@code UPDATE} or {@code DELETE} statements with an {@code IF} condition).
     *
     * <p>Two consistency levels belong to this category: {@link #SERIAL} and {@link #LOCAL_SERIAL}.
     *
     * @return whether this consistency level is {@link #SERIAL} or {@link #LOCAL_SERIAL}.
     * Statement#setSerialConsistencyLevel(aConsistencyLevel)
     * @see <a
     *     href="https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_ltwt_transaction_c.html">Lightweight
     *     transactions</a>
     */
    public boolean isSerial() {
        return this == SERIAL || this == LOCAL_SERIAL;
    }
}
