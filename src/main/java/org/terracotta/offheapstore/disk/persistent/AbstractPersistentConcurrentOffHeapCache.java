/*
 * Copyright 2015 cdennis.
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
package org.terracotta.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

/**
 *
 * @author cdennis
 */
public class AbstractPersistentConcurrentOffHeapCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> implements Persistent {

  private static final int MAGIC = 0x57415349;

  /**
   * Create a concurrent map using a default number of segments.
   *
   * @param segmentFactory factory used to create the map segments
   */
  public AbstractPersistentConcurrentOffHeapCache(Factory<? extends PinnableSegment<K, V>> segmentFactory) {
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
  public AbstractPersistentConcurrentOffHeapCache(Factory<? extends PinnableSegment<K, V>> segmentFactory, int concurrency) {
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
