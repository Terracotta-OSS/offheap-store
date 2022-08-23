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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptibleChannel;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class ReopeningInterruptibleChannel<T extends InterruptibleChannel> implements InterruptibleChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReopeningInterruptibleChannel.class);

  public static <T extends InterruptibleChannel> ReopeningInterruptibleChannel<T> create(Supplier<T> channelFactory) {
    return new ReopeningInterruptibleChannel<>(channelFactory);
  }

  private final Supplier<T> channelFactory;
  private final AtomicReference<T> currentChannel;

  private ReopeningInterruptibleChannel(Supplier<T> deviceFactory) {
    this.channelFactory = deviceFactory;
    this.currentChannel = new AtomicReference<>(deviceFactory.get());
  }

  public <R> R execute(IoOperation<T, R> operation) throws IOException {
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        T current = currentChannel.get();
        if (current == null) {
          throw new ClosedChannelException();
        } else {
          try {
            return operation.apply(current);
          } catch (ClosedChannelException e) {
            interrupted |= Thread.interrupted();

            if (e instanceof ClosedByInterruptException) {
              LOGGER.info("Interruption of this thread (" + Thread.currentThread() + ") caused premature closure of a channel");
            }
            T newDevice = channelFactory.get();
            if (!currentChannel.compareAndSet(current, newDevice)) {
              newDevice.close();
            } else {
              LOGGER.debug("Replacing channel " + current + " with " + newDevice + " due to premature closure");
            }
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

  }

  @Override
  public boolean isOpen() {
    return currentChannel.get() != null;
  }

  @Override
  public void close() throws IOException {
    T terminalDevice = currentChannel.getAndSet(null);
    if (terminalDevice != null) {
      terminalDevice.close();
    }
  }

  public interface IoOperation<T, R> {

    R apply(T t) throws IOException;

  }
}
