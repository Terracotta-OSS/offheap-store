/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.util;

import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RetryAssert {

    protected RetryAssert() {
        // static only class
    }

    public static <T> void assertBy(long time, TimeUnit unit, Callable<T> value, Matcher<? super T> matcher) {
        boolean interrupted = Thread.interrupted();
        try {
          long end = System.nanoTime() + unit.toNanos(time);
            for (long sleep = 10; ; sleep <<= 1L) {
                try {
                    Assert.assertThat(value.call(), matcher);
                    return;
                } catch (Throwable t) {
                    //ignore - wait for timeout
                }

                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    break;
                } else {
                    try {
                        Thread.sleep(Math.min(sleep, TimeUnit.NANOSECONDS.toMillis(remaining) + 1));
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            Assert.assertThat(value.call(), matcher);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
