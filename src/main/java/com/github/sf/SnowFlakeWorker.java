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
    /**
     * 时间纪元,用于表示业务起始时间,提升可用率
     */
    private final long INIT_EPOCH = 1652697002988L;
    /**
     * 避免碰撞,使用idc_id+worker_id组合来区分不同的集群节点
     */
    private long idcId;
    private long workerId;
    /**
     * idc_id的长度为5bit
     */
    private final long IDC_ID_BITS = 5L;
    /**
     * worker_id的长度为5bit
     */
    private final long WORKER_ID_BITS = 5L;
    /**
     * idc_id的最大值为31
     */
    private final long MAX_IDC_ID = ~(-1L << IDC_ID_BITS);
    /**
     * worker_id的最大值为31
     */
    private final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    /**
     * 自增id,理论4095/ms
     */
    private long sequence;
    /**
     * sequence的长度为12bit
     */
    private final long SEQUENCE_BITS = 12L;
    /**
     * sequence的最大值为4095
     */
    private final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);
    /**
     * 时间戳左移63bit-41bit=22bit
     */
    private final long TIMESTAMP_SHIFT = 22L;
    /**
     * idc_id左移63bit-(41bit+5bit)=17bit
     */
    private final long IDC_ID_SHIFT = 17L;
    /**
     * worker_id左移63bit-(41bit+5bit+5bit)=12bit
     */
    private final long WORKER_ID_SHIFT = 12L;
    private long lts = -1L;

    public SnowFlakeWorker(long idcId, long workerId) throws Throwable {
        if (idcId < 0 || idcId > MAX_IDC_ID) {
            throw new Throwable("idc_id input error");
        }
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new Throwable("worker_id input error");
        }
        this.idcId = idcId;
        this.workerId = workerId;
    }

    @Override
    public Long nextId() throws Throwable {
        return getSequenceId();
    }

    private synchronized Long getSequenceId() throws Throwable {
        var ct = System.currentTimeMillis();
        // 如果当前时间戳小于最后记录的时间戳则表示出现时间回拨
        if (ct < lts) {
            throw new Throwable("id generation failed");
        }
        // 1ms内最大生成4095个id
        if (ct == lts) {
            // 1000000000000(4096) & 0111111111111(4095) = 0
            sequence = (++sequence) & MAX_SEQUENCE;
            // sequenceId为0时表示同一ms内序列id已经用完需要等待进入下一个毫秒区间
            ct = 0 == sequence ? tilNextMillis() : ct;
        } else {
            sequence = 0;
        }
        lts = ct;
        // 位移拼接
        return ((ct - INIT_EPOCH) << TIMESTAMP_SHIFT)
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
