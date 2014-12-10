package com.terracottatech.offheapstore.storage.restartable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.terracottatech.frs.object.AbstractObjectManagerStripe;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;
import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.Segment;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;

public class OffHeapObjectManagerStripe<I> extends AbstractObjectManagerStripe<I, ByteBuffer, ByteBuffer> implements RestartableObject<I, ByteBuffer, ByteBuffer> {

  private final I identifier;
  private final AbstractConcurrentOffHeapMap<?, ?> concurrentMap;
  private final List<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> segments;
  
  @SuppressWarnings("unchecked")
  public OffHeapObjectManagerStripe(I identifier, OffHeapHashMap<?, ?> map) {
    this(identifier, Collections.<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>>singletonList((ObjectManagerSegment<I, ByteBuffer, ByteBuffer>) map.getStorageEngine()), null);
  }
  
  public OffHeapObjectManagerStripe(I identifier, AbstractConcurrentOffHeapMap<?, ?> map) {
    this(identifier, OffHeapObjectManagerStripe.<I>getRestartableSegments(map), map);
  }

  protected OffHeapObjectManagerStripe(I identifier, List<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> segments, AbstractConcurrentOffHeapMap<?, ?> map) {
    this.identifier = identifier;
    this.segments = Collections.<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>>unmodifiableList(segments);
    this.concurrentMap = map;
  }
  
  @Override
  public Collection<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> getSegments() {
    return segments;
  }

  @Override
  public void replayPut(ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    try {
      super.replayPut(frsBinaryKey, frsBinaryValue, lsn);
    } catch (OversizeMappingException e) {
      if (concurrentMap instanceof AbstractConcurrentOffHeapCache<?, ?>) {
        AbstractConcurrentOffHeapCache<?, ?> concurrentCache = (AbstractConcurrentOffHeapCache<?, ?>) concurrentMap;
        int hashcode = extractHashCode(frsBinaryKey);
        if (concurrentCache.handleOversizeMappingException(hashcode)) {
          try {
            super.replayPut(frsBinaryKey, frsBinaryValue, lsn);
            return;
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        }
        
        concurrentCache.writeLockAll();
        try {
          do {
            try {
              super.replayPut(frsBinaryKey, frsBinaryValue, lsn);
              return;
            } catch (OversizeMappingException ex) {
              e = ex;
            }
          } while (concurrentCache.handleOversizeMappingException(hashcode));
          throw e;
        } finally {
          concurrentCache.writeUnlockAll();
        }
      } else {
        throw e;
      }
    }
  }

  @Override
  protected ObjectManagerSegment<I, ByteBuffer, ByteBuffer> getSegmentFor(int hash, ByteBuffer key) {
    if (segments.size() == 1) {
      return segments.get(0);
    } else {
      return segments.get(concurrentMap.getIndexFor(hash));
    }
  }

  @Override
  protected int extractHashCode(ByteBuffer frsBinaryKey) {
    return RestartableStorageEngine.extractHashcode(frsBinaryKey);
  }

  @SuppressWarnings("unchecked")
  private static <I> List<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> getRestartableSegments(AbstractConcurrentOffHeapMap<?, ?> map) {
    ArrayList<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> result = new ArrayList<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>>();
    for (Segment<?, ?> segment : map.getSegments()) {
      result.add((ObjectManagerSegment<I, ByteBuffer, ByteBuffer>) ((OffHeapHashMap<?, ?>) segment).getStorageEngine());
    }
    return result;
  }

  @Override
  public void delete() {
    //no-op
  }

  @Override
  public I getId() {
    return identifier;
  }

  @Override
  public ObjectManagerStripe<I, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return this;
  }
}
