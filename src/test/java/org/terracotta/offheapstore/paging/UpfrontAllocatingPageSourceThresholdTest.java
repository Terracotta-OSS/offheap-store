/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package org.terracotta.offheapstore.paging;

import org.hamcrest.core.IsNull;
import org.hamcrest.number.OrderingComparison;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource.ThresholdDirection;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.util.MemoryUnit;
import org.terracotta.offheapstore.util.NoOpLock;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.core.Is.is;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertThat;

/**
 *
 * @author cdennis
 */
public class UpfrontAllocatingPageSourceThresholdTest {

  @Test
  public void testSimpleRisingThreshold() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    final AtomicBoolean rising = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.RISING, 128, () -> rising.set(true));

    assertThat(rising.getAndSet(false), is(false));
    Page a = source.allocate(256, false, false, null);
    assertThat(rising.getAndSet(false), is(true));
    source.free(a);
    assertThat(rising.getAndSet(false), is(false));
    Page b = source.allocate(256, false, false, null);
    assertThat(rising.getAndSet(false), is(true));
    Page c = source.allocate(256, false, false, null);
    source.free(b);
    source.free(c);
    assertThat(rising.getAndSet(false), is(false));
  }

  @Test
  public void testSimpleFallingThreshold() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    final AtomicBoolean falling = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.FALLING, 128, () -> falling.set(true));

    Page a = source.allocate(256, false, false, null);
    assertThat(falling.getAndSet(false), is(false));
    source.free(a);
    assertThat(falling.getAndSet(false), is(true));
    Page b = source.allocate(256, false, false, null);
    Page c = source.allocate(256, false, false, null);
    source.free(b);
    assertThat(falling.getAndSet(false), is(false));
    source.free(c);
    assertThat(falling.getAndSet(false), is(true));
  }

  @Test
  public void testMultipleRisingThresholds() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    final AtomicBoolean rising64 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.RISING, 64, () -> rising64.set(true));
    final AtomicBoolean rising128 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.RISING, 128, () -> rising128.set(true));

    assertThat(rising64.getAndSet(false), is(false));
    assertThat(rising128.getAndSet(false), is(false));
    Page a = source.allocate(96, false, false, null);
    assertThat(rising64.getAndSet(false), is(true));
    assertThat(rising128.getAndSet(false), is(false));
    Page b = source.allocate(48, false, false, null);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(rising128.getAndSet(false), is(true));
    source.free(a);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(rising128.getAndSet(false), is(false));
    Page c = source.allocate(96, false, false, null);
    assertThat(rising64.getAndSet(false), is(true));
    assertThat(rising128.getAndSet(false), is(true));
    source.free(b);
    source.free(c);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(rising128.getAndSet(false), is(false));
  }

  @Test
  public void testMultipleFallingThresholds() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    final AtomicBoolean falling64 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.FALLING, 64, () -> falling64.set(true));
    final AtomicBoolean falling128 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.FALLING, 128, () -> falling128.set(true));

    Page a = source.allocate(48, false, false, null);
    Page b = source.allocate(96, false, false, null);
    assertThat(falling64.getAndSet(false), is(false));
    assertThat(falling128.getAndSet(false), is(false));
    source.free(b);
    assertThat(falling64.getAndSet(false), is(true));
    assertThat(falling128.getAndSet(false), is(true));
    source.free(a);
    Page c = source.allocate(96, false, false, null);
    assertThat(falling64.getAndSet(false), is(false));
    assertThat(falling128.getAndSet(false), is(false));
    source.free(c);
    assertThat(falling64.getAndSet(false), is(true));
    assertThat(falling128.getAndSet(false), is(false));
    Page d = source.allocate(48, false, false, null);
    Page e = source.allocate(96, false, false, null);
    assertThat(falling64.getAndSet(false), is(false));
    assertThat(falling128.getAndSet(false), is(false));
    source.free(d);
    assertThat(falling64.getAndSet(false), is(false));
    assertThat(falling128.getAndSet(false), is(true));
    source.free(e);
    assertThat(falling64.getAndSet(false), is(true));
    assertThat(falling128.getAndSet(false), is(false));
  }

  @Test
  public void testRisingAndFallingThresholds() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    final AtomicBoolean rising64 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.RISING, 64, () -> rising64.set(true));
    final AtomicBoolean falling64 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.FALLING, 64, () -> falling64.set(true));

    Page a = source.allocate(32, false, false, null);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(false));
    Page b = source.allocate(64, false, false, null);
    assertThat(rising64.getAndSet(false), is(true));
    assertThat(falling64.getAndSet(false), is(false));
    source.free(b);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(true));
    source.free(a);
    assertThat(rising64.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(false));
  }

  @Test
  public void testThresholdsDuringSteal() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    SimpleOwner owner = new SimpleOwner();
    OffHeapStorageArea storage = new OffHeapStorageArea(PointerSize.INT, owner, source, 128, false, true);
    owner.bind(storage);

    final AtomicBoolean rising512 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.RISING, 512, () -> rising512.set(true));
    final AtomicBoolean falling64 = new AtomicBoolean();
    source.addAllocationThreshold(ThresholdDirection.FALLING, 64, () -> falling64.set(true));

    long victim = storage.allocate(128);
    assertThat(victim, OrderingComparison.greaterThanOrEqualTo(0L));
    assertThat(rising512.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(false));

    Page thief = source.allocate(1024, true, false, null);
    assertThat(thief, IsNull.notNullValue());
    assertThat(storage.getAllocatedMemory(), is(0L));
    assertThat(rising512.getAndSet(false), is(true));
    assertThat(falling64.getAndSet(false), is(true));

    long fail = storage.allocate(128);
    assertThat(fail, is(-1L));
    assertThat(storage.getAllocatedMemory(), is(0L));
    assertThat(rising512.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(false));

    source.free(thief);
    assertThat(storage.getAllocatedMemory(), is(0L));
    assertThat(source.getAllocatedSize(), is(0L));
    assertThat(rising512.getAndSet(false), is(false));
    assertThat(falling64.getAndSet(false), is(true));
  }

  @Test
  public void testActionThatThrows() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(1), MemoryUnit.KILOBYTES.toBytes(1));
    source.addAllocationThreshold(ThresholdDirection.RISING, 128, () -> {
      throw new Error();
    });

    Page a = source.allocate(256, false, false, null);
    assertThat(a, IsNull.notNullValue());
    assertThat(source.getAllocatedSize(), is(256L));
    source.free(a);
  }

  static class SimpleOwner implements OffHeapStorageArea.Owner {

    private OffHeapStorageArea storage;

    public void bind(OffHeapStorageArea storage) {
      this.storage = storage;
    }

    @Override
    public Collection<Long> evictAtAddress(long address, boolean shrink) {
      storage.free(address);
      return singleton(address);
    }

    @Override
    public Lock writeLock() {
      return NoOpLock.INSTANCE;
    }

    @Override
    public boolean isThief() {
      return false;
    }

    @Override
    public boolean moved(long shift, long pointer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int sizeOf(long shift) {
      throw new UnsupportedOperationException();
    }
  }
}
