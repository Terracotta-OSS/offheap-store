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

import org.terracotta.offheapstore.util.Retryer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 *
 * @author cdennis
 */
public class RetryerTest {

  @Test(expected = IllegalArgumentException.class)
  public void testIaeOnNullTimeUnit() {
    new Retryer(1, 2, null, defaultThreadFactory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIaeOnNullThreadFactory() {
    new Retryer(1, 2, MILLISECONDS, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIaeOnIllegalMinDelay() {
    new Retryer(0, 2, MILLISECONDS, defaultThreadFactory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIaeOnIllegalMaxDelay() {
    new Retryer(2, 1, MILLISECONDS, defaultThreadFactory());
  }

  @Test
  public void testSuccessfullTaskIsNotRetried() {
    Retryer retryer = new Retryer(1, 1, TimeUnit.MILLISECONDS, defaultThreadFactory());
    try {
      Runnable task = mock(Runnable.class);
      retryer.completeAsynchronously(task);
      verify(task, timeout(100).times(1)).run();
    } finally {
      retryer.shutdownNow();
    }
  }

  @Test
  public void testUnsuccessfullTaskIsRetried() {
    Retryer retryer = new Retryer(1, 1, TimeUnit.MILLISECONDS, defaultThreadFactory());
    try {
      Runnable task = mock(Runnable.class);
      doThrow(new IllegalStateException()).doNothing().when(task).run();

      retryer.completeAsynchronously(task);

      verify(task, timeout(100).times(2)).run();
    } finally {
      retryer.shutdownNow();
    }
  }

  @Test
  public void testRetriesBackOffCorrectly() {
    Retryer retryer = new Retryer(100, 1000, TimeUnit.MILLISECONDS, defaultThreadFactory());
    try {
      final Map<Integer, Long> executionTimes = new ConcurrentHashMap<>();

      Runnable task = mock(Runnable.class);
      doAnswer(new Answer() {

        private volatile int executions;

        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          executionTimes.put(executions, System.currentTimeMillis());
          switch (executions++) {
            case 0:
            case 1:
              throw new IllegalStateException();
            default:
              return null;
          }
        }
      }).when(task).run();

      retryer.completeAsynchronously(task);

      verify(task, timeout(500).times(3)).run();

      long deltaOne = executionTimes.get(1) - executionTimes.get(0);
      long deltaTwo = executionTimes.get(2) - executionTimes.get(1);

      assertThat(deltaTwo, greaterThan(deltaOne));
    } finally {
      retryer.shutdownNow();
    }
  }

  @Test
  public void testShutdownRetryerRefuses() {
    Retryer retryer = new Retryer(1, 1, TimeUnit.MILLISECONDS, defaultThreadFactory());
    retryer.shutdownNow();

    try {
      retryer.completeAsynchronously(() -> {
      });
      fail("Expected RejectedExecutionException");
    } catch (RejectedExecutionException e) {
      //expected
    }
  }
}
