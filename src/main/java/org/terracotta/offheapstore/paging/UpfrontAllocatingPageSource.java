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
package org.terracotta.offheapstore.paging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.buffersource.BufferSource;
import org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator;
import org.terracotta.offheapstore.util.DebuggingUtils;
import org.terracotta.offheapstore.util.MemoryUnit;
import org.terracotta.offheapstore.util.PhysicalMemory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator.Packing.CEILING;
import static org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator.Packing.FLOOR;

/**
 * An upfront allocating direct byte buffer source.
 * <p>
 * This buffer source implementation allocates all of its required storage
 * up-front in fixed size chunks.  Runtime allocations are then satisfied using
 * slices from these initial chunks.
 *
 * @author Chris Dennis
 */
public class UpfrontAllocatingPageSource implements PageSource {

    public static final String ALLOCATION_LOG_LOCATION = UpfrontAllocatingPageSource.class.getName() + ".allocationDump";

    private static final Logger LOGGER = LoggerFactory.getLogger(UpfrontAllocatingPageSource.class);
    private static final Comparator<Page> REGION_COMPARATOR = new Comparator<Page>() {
      @Override
      public int compare(Page a, Page b) {
        if (a.address() == b.address()) {
          return a.size() - b.size();
        } else {
          return a.address() - b.address();
        }
      }
    };

    private final SortedMap<Long, Runnable> risingThresholds = new TreeMap<Long, Runnable>();
    private final SortedMap<Long, Runnable> fallingThresholds = new TreeMap<Long, Runnable>();

    private final List<PowerOfTwoAllocator> sliceAllocators = new ArrayList<PowerOfTwoAllocator>();
    private final List<PowerOfTwoAllocator> victimAllocators = new ArrayList<PowerOfTwoAllocator>();

    private final List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

    /*
     * TODO : currently the TreeSet along with the comparator above works for the
     * subSet queries due to the alignment properties of the region allocation
     * being used here.  I more flexible implementation might involve switching
     * to using an AATreeSet subclass - that would also require me to finish
     * writing the subSet implementation for that class.
     */
    private final List<NavigableSet<Page>> victims = new ArrayList<NavigableSet<Page>>();

    private volatile int availableSet = ~0;

    /**
     * Create an up-front allocating buffer source of {@code toAllocate} total bytes, in
     * {@code chunkSize} byte chunks.
     *
     * @param source     source from which initial buffers will be allocated
     * @param toAllocate total space to allocate in bytes
     * @param chunkSize  chunkSize size to allocate in bytes
     */
    public UpfrontAllocatingPageSource(BufferSource source, long toAllocate, int chunkSize) {
        this(source, toAllocate, chunkSize, -1, true);
    }

    /**
     * Create an up-front allocating buffer source of {@code toAllocate} total bytes, in
     * maximally sized chunks, within the given bounds.
     *
     * @param source     source from which initial buffers will be allocated
     * @param toAllocate total space to allocate in bytes
     * @param maxChunk   the largest chunk size in bytes
     * @param minChunk   the smallest chunk size in bytes
     */
    public UpfrontAllocatingPageSource(BufferSource source, long toAllocate, int maxChunk, int minChunk) {
        this(source, toAllocate, maxChunk, minChunk, false);
    }

  /**
   * Create an up-front allocating buffer source of {@code toAllocate} total bytes, in
   * maximally sized chunks, within the given bounds.
   * <p>
   * By default we try to allocate chunks of {@code maxChunk} size. However, unless {@code fixed} is true, in case of
   * allocation failure, we will try to allocate half-smaller chunks. We do not allocate chunks smaller than {@code minChunk}
   * though.
   *
   * @param source     source from which initial buffers will be allocated
   * @param toAllocate total space to allocate in bytes
   * @param maxChunk   the largest chunk size in bytes
   * @param minChunk   the smallest chunk size in bytes
   * @param fixed      if the chunks should all be of size {@code maxChunk} or can be smaller
   */
    private UpfrontAllocatingPageSource(BufferSource source, long toAllocate, int maxChunk, int minChunk, boolean fixed) {
        Long totalPhysical = PhysicalMemory.totalPhysicalMemory();
        Long freePhysical = PhysicalMemory.freePhysicalMemory();
        if (totalPhysical != null && toAllocate > totalPhysical) {
          throw new IllegalArgumentException("Attempting to allocate " + DebuggingUtils.toBase2SuffixedString(toAllocate) + "B of memory "
                  + "when the host only contains " + DebuggingUtils.toBase2SuffixedString(totalPhysical) + "B of physical memory");
        }
        if (freePhysical != null && toAllocate > freePhysical) {
          LOGGER.warn("Attempting to allocate {}B of offheap when there is only {}B of free physical memory - some paging will therefore occur.",
                  DebuggingUtils.toBase2SuffixedString(toAllocate), DebuggingUtils.toBase2SuffixedString(freePhysical));
        }

        LOGGER.debug("Allocating {}B in chunks", DebuggingUtils.toBase2SuffixedString(toAllocate));

        for (ByteBuffer buffer : allocateBackingBuffers(source, toAllocate, maxChunk, minChunk, fixed)) {
          sliceAllocators.add(new PowerOfTwoAllocator(buffer.capacity()));
          victimAllocators.add(new PowerOfTwoAllocator(buffer.capacity()));
          victims.add(new TreeSet<Page>(REGION_COMPARATOR));
          buffers.add(buffer);
        }
    }

    /**
     * Allocates a byte buffer of at least the given size.
     * <p>
     * This {@code BufferSource} is limited to allocating regions that are a power
     * of two in size.  Supplied sizes are therefore rounded up to the next
     * largest power of two.
     *
     * @return a buffer of at least the given size
     */
    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      if (thief) {
        return allocateAsThief(size, victim, owner);
      } else {
        return allocateFromFree(size, victim, owner);
      }
    }

    private Page allocateAsThief(final int size, boolean victim, OffHeapStorageArea owner) {
      Page free = allocateFromFree(size, victim, owner);

      if (free != null) {
        return free;
      }

      //do thieving here...
      PowerOfTwoAllocator victimAllocator = null;
      PowerOfTwoAllocator sliceAllocator = null;
      List<Page> targets = Collections.emptyList();
      Collection<AllocatedRegion> tempHolds = new ArrayList<AllocatedRegion>();
      Map<OffHeapStorageArea, Collection<Page>> releases = new IdentityHashMap<OffHeapStorageArea, Collection<Page>>();

      synchronized (this) {
        for (int i = 0; i < victimAllocators.size(); i++) {
          int address = victimAllocators.get(i).find(size, victim ? CEILING : FLOOR);
          if (address >= 0) {
            victimAllocator = victimAllocators.get(i);
            sliceAllocator = sliceAllocators.get(i);
            targets = findVictimPages(i, address, size);

            //need to claim everything that falls within the range of our allocation
            int claimAddress = address;
            for (Page p : targets) {
              victimAllocator.claim(p.address(), p.size());
              int claimSize = p.address() - claimAddress;
              if (claimSize > 0) {
                tempHolds.add(new AllocatedRegion(claimAddress, claimSize));
                sliceAllocator.claim(claimAddress, claimSize);
                victimAllocator.claim(claimAddress, claimSize);
              }
              claimAddress = p.address() + p.size();
            }
            int claimSize = (address + size) - claimAddress;
            if (claimSize > 0) {
              tempHolds.add(new AllocatedRegion(claimAddress, claimSize));
              sliceAllocator.claim(claimAddress, claimSize);
              victimAllocator.claim(claimAddress, claimSize);
            }
            break;
          }
        }

        for (Page p : targets) {
          OffHeapStorageArea a = p.binding();
          Collection<Page> c = releases.get(a);
          if (c == null) {
            c = new LinkedList<Page>();
            c.add(p);
            releases.put(a, c);
          } else {
            c.add(p);
          }
        }
      }

      /*
       * Drop the page source synchronization here to prevent deadlock against
       * map/cache threads.
       */
      Collection<Page> results = new LinkedList<Page>();
      for (Entry<OffHeapStorageArea, Collection<Page>> e : releases.entrySet()) {
        OffHeapStorageArea a = e.getKey();
        Collection<Page> p = e.getValue();
        results.addAll(a.release(p));
      }

      List<Page> failedReleases = new ArrayList<Page>();
      synchronized (this) {
        for (AllocatedRegion r : tempHolds) {
          sliceAllocator.free(r.address, r.size);
          victimAllocator.free(r.address, r.size);
        }

        if (results.size() == targets.size()) {
          for (Page p : targets) {
            victimAllocator.free(p.address(), p.size());
            free(p);
          }
          return allocateFromFree(size, victim, owner);
        } else {
          for (Page p : targets) {
            if (results.contains(p)) {
              victimAllocator.free(p.address(), p.size());
              free(p);
            } else {
              failedReleases.add(p);
            }
          }
        }
      }

      try {
        return allocateAsThief(size, victim, owner);
      } finally {
        synchronized (this) {
          for (Page p : failedReleases) {
            //this is just an ugly way of doing an identity equals based contains
            if (victims.get(p.index()).floor(p) == p) {
              victimAllocator.free(p.address(), p.size());
            }
          }
        }
      }
    }

    private List<Page> findVictimPages(int chunk, int address, int size) {
      return new ArrayList<Page>(victims.get(chunk).subSet(new Page(null, -1, address, null),
                                                     new Page(null, -1, address + size, null)));
    }

    private Page allocateFromFree(int size, boolean victim, OffHeapStorageArea owner) {
        if (Integer.bitCount(size) != 1) {
            int rounded = Integer.highestOneBit(size) << 1;
            LOGGER.debug("Request to allocate {}B will allocate {}B", size, DebuggingUtils.toBase2SuffixedString(rounded));
            size = rounded;
        }

        if (isUnavailable(size)) {
            return null;
        }

        synchronized (this) {
            for (int i = 0; i < sliceAllocators.size(); i++) {
                int address = sliceAllocators.get(i).allocate(size, victim ? CEILING : FLOOR);
                if (address >= 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Allocating a {}B buffer from chunk {} &{}", new Object[]{DebuggingUtils.toBase2SuffixedString(size), i, address});
                    }
                    ByteBuffer b = ((ByteBuffer) buffers.get(i).limit(address + size).position(address)).slice();
                    Page p = new Page(b, i, address, owner);
                    if (victim) {
                      victims.get(i).add(p);
                    } else {
                      victimAllocators.get(i).claim(address, size);
                    }
                    if (!risingThresholds.isEmpty()) {
                      long allocated = getAllocatedSize();
                      fireThresholds(allocated - size, allocated);
                    }
                    return p;
                }
            }
            markUnavailable(size);
            return null;
        }
    }

    /**
     * Frees the supplied buffer.
     * <p>
     * If the given buffer was not allocated by this source or has already been
     * freed then an {@code AssertionError} is thrown.
     */
    @Override
    public synchronized void free(Page page) {
        if (page.isFreeable()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Freeing a {}B buffer from chunk {} &{}", new Object[]{DebuggingUtils.toBase2SuffixedString(page.size()), page.index(), page.address()});
            }
            markAllAvailable();
            sliceAllocators.get(page.index()).free(page.address(), page.size());
            victims.get(page.index()).remove(page);
            victimAllocators.get(page.index()).tryFree(page.address(), page.size());
            if (!fallingThresholds.isEmpty()) {
              long allocated = getAllocatedSize();
              fireThresholds(allocated + page.size(), allocated);
            }
        }
    }

    public synchronized long getAllocatedSize() {
        long sum = 0;
        for (PowerOfTwoAllocator a : sliceAllocators) {
            sum += a.occupied();
        }
        return sum;
    }

    public long getAllocatedSizeUnSync() {
        long sum = 0;
        for (PowerOfTwoAllocator a : sliceAllocators) {
            sum += a.occupied();
        }
        return sum;
    }

    private boolean isUnavailable(int size) {
        return (availableSet & size) == 0;
    }

    private synchronized void markAllAvailable() {
        availableSet = ~0;
    }

    private synchronized void markUnavailable(int size) {
        availableSet &= ~size;
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder("UpfrontAllocatingPageSource");
        for (int i = 0; i < buffers.size(); i++) {
            sb.append("\nChunk ").append(i + 1).append('\n');
            sb.append("Size             : ").append(DebuggingUtils.toBase2SuffixedString(buffers.get(i).capacity())).append("B\n");
            sb.append("Free Allocator   : ").append(sliceAllocators.get(i)).append('\n');
            sb.append("Victim Allocator : ").append(victimAllocators.get(i));
        }
        return sb.toString();

    }

    private synchronized void fireThresholds(long incoming, long outgoing) {
      Collection<Runnable> thresholds;
      if (outgoing > incoming) {
        thresholds = risingThresholds.subMap(incoming, outgoing).values();
      } else if (outgoing < incoming) {
        thresholds = fallingThresholds.subMap(outgoing, incoming).values();
      } else {
        thresholds = Collections.emptyList();
      }

      for (Runnable r : thresholds) {
        try {
          r.run();
        } catch (Throwable t) {
          LOGGER.error("Throwable thrown by threshold action", t);
        }
      }
    }

    /**
     * Adds an allocation threshold action.
     * <p>
     * There can be only a single action associated with each unique direction
     * and threshold combination.  If an action is already associated with the
     * supplied combination then the action is replaced by the new action and the
     * old action is returned.
     * <p>
     * Actions are fired on passing through the supplied threshold and are called
     * synchronously with the triggering allocation.  This means care must be taken
     * to avoid mutating any map that uses this page source from within the action
     * otherwise deadlocks may result.  Exceptions thrown by the action will be
     * caught and logged by the page source and will not be propagated on the
     * allocating thread.
     *
     * @param direction new actions direction
     * @param threshold new actions threshold level
     * @param action fired on breaching the threshold
     * @return the replaced action or {@code null} if no action was present.
     */
    public synchronized Runnable addAllocationThreshold(ThresholdDirection direction, long threshold, Runnable action) {
      switch (direction) {
        case RISING:
          return risingThresholds.put(threshold, action);
        case FALLING:
          return fallingThresholds.put(threshold, action);
      }
      throw new AssertionError();
    }

    /**
     * Removes an allocation threshold action.
     * <p>
     * Removes the allocation threshold action for the given level and direction.
     *
     * @param direction registered actions direction
     * @param threshold registered actions threshold level
     * @return the removed condition or {@code null} if no action was present.
     */
    public synchronized Runnable removeAllocationThreshold(ThresholdDirection direction, long threshold) {
      switch (direction) {
        case RISING:
          return risingThresholds.remove(threshold);
        case FALLING:
          return fallingThresholds.remove(threshold);
      }
      throw new AssertionError();
    }

  /**
   * Allocate multiple buffers to fulfill the requested memory {@code toAllocate}. We first divide {@code toAllocate} in
   * chunks of size {@maxChunk} and try to allocate them in parallel on all available processors. If one chunk fails to be
   * allocated, we try to allocate two chunks of {@code maxChunk / 2}. If this allocation fails, we continue dividing until
   * we reach of size of {@code minChunk}. If at that moment, the allocation still fails, an {@code IllegalArgumentException}
   * is thrown.
   * <p>
   * When {@code fixed} is requested, we will only allocated buffers of {@code maxChunk} size. If allocation fails, an
   * {@code IllegalArgumentException} is thrown without any division.
   * <p>
   * If the allocation is interrupted, the method will exit and return what was allocated so far. The interrupt flag is
   * set.
   *
   * @param source source used to allocate memory buffers
   * @param toAllocate total amount of memory to allocate
   * @param maxChunk maximum size of a buffer. This is the targeted size for all buffers if everything goes well
   * @param minChunk minimum buffer size allowed
   * @param fixed if all buffers should have a the same size (except the last one with {@code toAllocate % maxChunk != 0}, if true, {@code minChunk} isn't used
   * @return the list of allocated buffers
   * @throws IllegalArgumentException when we fail to allocate the requested memory
   */
    private static Collection<ByteBuffer> allocateBackingBuffers(final BufferSource source, long toAllocate, int maxChunk, final int minChunk, final boolean fixed) {

      final PrintStream allocatorLog;
      try {
      final Collection<ByteBuffer> buffers = new ArrayList<ByteBuffer>((int)(toAllocate / maxChunk + 10)); // guess the number of buffers and add some padding just in case
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to create allocator log", e);
      }

      final long start = (LOGGER.isDebugEnabled() ? System.nanoTime() : 0);

      if (allocatorLog != null) {
        allocatorLog.printf("timestamp,duration,size,physfree,totalswap,freeswap,committed%n");
      }

      ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        List<Future<Long>> futures = new ArrayList<Future<Long>>((int)(toAllocate / maxChunk + 1));
      List<Future<?>> futures = new ArrayList<Future<?>>((int) toAllocate / maxChunk + 1);

      long allocated = 0;

      while(allocated < toAllocate) {
        final int currentChunkSize = (int) Math.min(maxChunk, toAllocate - allocated);
        futures.add(executorService.submit(new Runnable() {
          @Override
          public void run() {
            bufferAllocation(source, currentChunkSize, minChunk, fixed, allocatorLog, start, buffers);
          }
        }));
        allocated += currentChunkSize;
      }

      executorService.shutdown();

      for(Future<?> future : futures) {
        try {
          future.get(); // no result to retrieve. However, we want to know if allocation failed
        } catch(ExecutionException e) {
          if(e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      if (allocatorLog != null) {
        allocatorLog.close();
      }

      if(LOGGER.isDebugEnabled()) {
        long duration = System.nanoTime() - start;
        LOGGER.debug("Took {} ms to create off-heap storage of {}B.", TimeUnit.NANOSECONDS.toMillis(duration), DebuggingUtils.toBase2SuffixedString(toAllocate));
      }

      return Collections.unmodifiableCollection(buffers);
    }

  private static void bufferAllocation(BufferSource source, int toAllocate, int minChunk, boolean fixed, PrintStream allocatorLog, long start, Collection<ByteBuffer> buffers) {
    long allocated = 0;
    long currentChunkSize = toAllocate;

    while (allocated < toAllocate) {
      long blockStart = System.nanoTime();
      int currentAllocation = (int)Math.min(currentChunkSize, (toAllocate - allocated));
      ByteBuffer b = source.allocateBuffer(currentAllocation);
      long blockDuration = System.nanoTime() - blockStart;

      if (b == null) {
        if (fixed || (currentChunkSize >>> 1) < minChunk) {
          throw new IllegalArgumentException("An attempt was made to allocate more off-heap memory than the JVM can allow." +
                                             " The limit on off-heap memory size is given by the -XX:MaxDirectMemorySize command (or equivalent).");
        }

        // In case of failure, we try half the allocation size. It might pass if memory fragmentation caused the failure
        currentChunkSize >>>= 1;

        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("Allocated failed at {}B, trying  {}B chunks.", DebuggingUtils.toBase2SuffixedString(currentAllocation), DebuggingUtils.toBase2SuffixedString(currentChunkSize));
        }
      } else {
        synchronized (buffers) {
          buffers.add(b);
        }
        allocated += currentAllocation;

        if (allocatorLog != null) {
          allocatorLog.printf("%d,%d,%d,%d,%d,%d,%d%n", System.nanoTime() - start, blockDuration, currentAllocation, PhysicalMemory.freePhysicalMemory(), PhysicalMemory.totalSwapSpace(), PhysicalMemory.freeSwapSpace(), PhysicalMemory.ourCommittedVirtualMemory());
        }
      }
    }
  }

  private static PrintStream createAllocatorLog(long max, int maxChunk, int minChunk) throws IOException {
      String path = System.getProperty(ALLOCATION_LOG_LOCATION);
      if (path == null) {
        return null;
      } else {
        File allocatorLogFile = File.createTempFile("allocation", ".csv", new File(path));
        PrintStream allocatorLogStream = new PrintStream(allocatorLogFile, "US-ASCII");
        allocatorLogStream.printf("Timestamp: %s%n", new Date());
        allocatorLogStream.printf("Allocating: %sB%n",DebuggingUtils.toBase2SuffixedString(max));
        allocatorLogStream.printf("Max Chunk: %sB%n",DebuggingUtils.toBase2SuffixedString(maxChunk));
        allocatorLogStream.printf("Min Chunk: %sB%n",DebuggingUtils.toBase2SuffixedString(minChunk));
        return allocatorLogStream;
      }
    }

    public enum ThresholdDirection {
      RISING, FALLING;
    }

    static class AllocatedRegion {

        private final int address;
        private final int size;

        AllocatedRegion(int address, int size) {
            this.address = address;
            this.size = size;
        }
    }
}
