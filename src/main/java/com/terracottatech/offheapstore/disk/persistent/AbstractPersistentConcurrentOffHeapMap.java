/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.terracottatech.offheapstore.Segment;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author cdennis
 */
public abstract class AbstractPersistentConcurrentOffHeapMap<K, V> extends AbstractConcurrentOffHeapMap<K, V> implements Persistent {

  private static final int MAGIC = 0x57415348;

  /**
   * Create a concurrent map using a default number of segments.
   *
   * @param segmentFactory factory used to create the map segments
   */
  public AbstractPersistentConcurrentOffHeapMap(Factory<? extends Segment<K, V>> segmentFactory, boolean latencyMonitoring) {
    super(segmentFactory);
  }

  /**
   * Create a concurrent map with a defined number of segments.
   *
   * @param segmentFactory factory used to create the map segments
   * @param concurrency number of segments in the map
   * @throws IllegalArgumentException if the supplied number of segments is
   * negative
   */
  public AbstractPersistentConcurrentOffHeapMap(Factory<? extends Segment<K, V>> segmentFactory, int concurrency, boolean latencyMonitoring) {
    super(segmentFactory, concurrency);
  }

  @Override
  public void flush() throws IOException {
    for (Segment<?, ?> s : segments) {
      ((Persistent) s).flush();
    }
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    output.writeInt(MAGIC);
    output.writeInt(segments.length);
    for (Segment<?, ?> s : segments) {
      ((Persistent) s).persist(output);
    }
  }

  @Override
  public void close() throws IOException {
    for (Segment<?, ?> s : segments) {
      ((Persistent) s).close();
    }
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    for (Segment<?, ?> s : segments) {
      ((Persistent) s).bootstrap(input);
    }
  }

  protected static int readSegmentCount(ObjectInput input) throws IOException {
    if (input.readInt() != MAGIC) {
      throw new IOException("Wrong magic number");
    }
    return input.readInt();
  }
}
