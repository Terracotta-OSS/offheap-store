/*
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptibleChannel;
import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReopeningInterruptibleChannelTest {

  @Test
  public void testStraightExecution() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    reopeningChannel.execute(SimpleInterruptibleChannel::operation);

    verify(delegate, times(1)).operation();
  }

  @Test
  public void testInterruptIsPreserved() throws IOException {
    Thread.currentThread().interrupt();

    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> mock(SimpleInterruptibleChannel.class));

    reopeningChannel.execute(SimpleInterruptibleChannel::operation);
    assertThat(Thread.interrupted(), is(true));
  }

  @Test
  public void testIOExceptionPropagates() throws IOException {
    IOException ioException = new IOException();
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    when(delegate.operation()).thenThrow(ioException);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    try {
      reopeningChannel.execute(SimpleInterruptibleChannel::operation);
      fail("Expected IOException");
    } catch (IOException e) {
      assertThat(e, is(sameInstance(ioException)));
    }
  }

  @Test
  public void testClosedChannelExceptionRefetches() throws IOException {
    SimpleInterruptibleChannel closed = mock(SimpleInterruptibleChannel.class);
    when(closed.operation()).thenThrow(ClosedChannelException.class);
    SimpleInterruptibleChannel open = mock(SimpleInterruptibleChannel.class);
    Object result = new Object();
    when(open.operation()).thenReturn(result);
    Iterator<SimpleInterruptibleChannel> sequence = asList(closed, open).iterator();
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(sequence::next);

    assertThat(reopeningChannel.execute(SimpleInterruptibleChannel::operation), is(sameInstance(result)));

    verify(closed, times(1)).operation();
    verify(open, times(1)).operation();
  }

  @Test
  public void testClosingClosesDelegate() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    reopeningChannel.close();

    verify(delegate, times(1)).close();
  }

  @Test
  public void testClosingIsIdempotent() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    reopeningChannel.close();
    reopeningChannel.close();

    verify(delegate, times(1)).close();
  }

  @Test
  public void testClosedDevicesThrows() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    reopeningChannel.close();

    try {
      reopeningChannel.execute(SimpleInterruptibleChannel::operation);
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException e) {
      //expected
    }
  }

  @Test
  public void testOpenInitially() {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    assertThat(reopeningChannel.isOpen(), is(true));
  }

  @Test
  public void testClosedAfterExplicitlyClosed() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    reopeningChannel.close();

    assertThat(reopeningChannel.isOpen(), is(false));
  }

  @Test
  public void testOpenDespiteDelegateClosure() throws IOException {
    SimpleInterruptibleChannel delegate = mock(SimpleInterruptibleChannel.class);
    when(delegate.operation()).thenThrow(ClosedChannelException.class);
    ReopeningInterruptibleChannel<SimpleInterruptibleChannel> reopeningChannel = ReopeningInterruptibleChannel.create(() -> delegate);

    assertThat(reopeningChannel.isOpen(), is(true));
  }

  interface SimpleInterruptibleChannel extends InterruptibleChannel {

    Object operation() throws IOException;
  }
}

