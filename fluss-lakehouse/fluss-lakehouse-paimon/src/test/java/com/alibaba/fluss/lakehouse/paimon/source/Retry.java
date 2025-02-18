/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon.source;

import java.util.concurrent.Callable;

import static java.util.concurrent.locks.LockSupport.parkNanos;

public class Retry {

    private final int retries;
    private final long delayMillis;

    public Retry(int retries, long delayMillis) {
        this.retries = retries;
        this.delayMillis = delayMillis;
    }

    public <T> T execute(Callable<T> callable) throws Exception {
        for (int i = 0; true; i++) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (i >= retries) {
                    throw e;
                } else {
                    parkNanos((long) Math.sqrt(i) * delayMillis * 1000000);
                }
            }
        }
    }
}
