/*
 * Copyright 2019-2119 gao_xianglong@sina.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sf;

/**
 * SnowFlake by java
 *
 * @author gao_xianglong@sina.com
 * @version 0.1-SNAPSHOT
 * @date created in 2022/5/16 23:50
 */
public class SnowFlakeWorker implements IdWorker<Long> {
    private final long INIT_TIMESTAMP = 1652962585889L;
    private final long IDC_ID_BITS = 5L;
    private final long WORKER_ID_BITS = 5L;
    private final long TIMESTAMP_BITS = 41L;
    private final long SEQUENCE_BITS = 12L;
    private final long MAX_IDC_ID = ~(-1L << IDC_ID_BITS);
    private final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);
    private final long AVAILABLE = 63L;
    /**
     * <<22bit
     */
    private final long TIMESTAMP_SHIFT = AVAILABLE - TIMESTAMP_BITS;
    /**
     * <<17bit
     */
    private final long IDC_ID_SHIFT = AVAILABLE - (TIMESTAMP_BITS + IDC_ID_BITS);
    /**
     * <<12bit
     */
    private final long WORKER_ID_SHIFT = AVAILABLE - (TIMESTAMP_BITS + IDC_ID_BITS + WORKER_ID_BITS);
    private long idcId;
    private long workerId;
    private long sequence;
    private long lts = -1L;

    public SnowFlakeWorker(long idcId, long workerId) throws Throwable {
        check(idcId, workerId);
        this.idcId = idcId;
        this.workerId = workerId;
    }

    private void check(long idcId, long workerId) throws Throwable {
        if (idcId < 0 || idcId > MAX_IDC_ID) {
            throw new Throwable("idc_id input error");
        }
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new Throwable("worker_id input error");
        }
    }

    @Override
    public Long nextId() throws Throwable {
        return getSequenceId();
    }

    private synchronized long getSequenceId() throws Throwable {
        var ct = System.currentTimeMillis();
        if (ct < lts) {
            throw new Throwable("ct < lts");
        }
        if (ct == lts) {
            sequence = (++sequence) & MAX_SEQUENCE;
            ct = 0 == sequence ? tilNextMillis() : ct;
        } else {
            sequence = 0;
        }
//        if (ct == lts) {
//            sequence = (++sequence) & MAX_SEQUENCE;
//            if (0 == sequence) {
//                sequence = (int) (Math.random() * 100);
//                ct = tilNextMillis();
//            }
//        } else {
//            sequence = (int) (Math.random() * 100);
//        }
        lts = ct;
        return ((ct - INIT_TIMESTAMP) << TIMESTAMP_SHIFT)
                | (idcId << IDC_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;

    }

    private long tilNextMillis() {
        var ct = System.currentTimeMillis();
        while (ct <= lts) {
            ct = System.currentTimeMillis();
        }
        return ct;
    }
}