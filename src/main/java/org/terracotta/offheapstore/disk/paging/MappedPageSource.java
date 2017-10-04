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
package org.terracotta.offheapstore.disk.paging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.util.DebuggingUtils;
import org.terracotta.offheapstore.util.Retryer;

/**
 *
 * @author Chris Dennis
 */
public class MappedPageSource implements PageSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MappedPageSource.class);
  private static final Retryer ASYNC_FLUSH_EXECUTOR = new Retryer(10, 600, TimeUnit.SECONDS, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, "MappedByteBufferSource Async Flush Thread");
      t.setDaemon(true);
      return t;
    }
  });

  private final File file;
  private final RandomAccessFile raf;
  private final FileChannel channel;
  private final PowerOfTwoFileAllocator allocator;

  private final IdentityHashMap<MappedPage, Long> pages = new IdentityHashMap<>();
  private final Map<Long, AllocatedRegion> allocated = new HashMap<>();

  public MappedPageSource(File file) throws IOException {
    this(file, true);
  }

  public MappedPageSource(File file, long size) throws IOException {
    this(file, true, size);
  }

  public MappedPageSource(File file, boolean truncate) throws IOException {
    this(file, truncate, Long.MAX_VALUE);
  }

  public MappedPageSource(File file, boolean truncate, long size) throws IOException {
    if (!file.createNewFile() && file.isDirectory()) {
      throw new IOException("File already exists and is a directory");
    }
    this.file = file;
    this.raf = new RandomAccessFile(file, "rw");
    this.channel = raf.getChannel();
    if (truncate) {
      try {
        channel.truncate(0);
      } catch (IOException e) {
        LOGGER.info("Exception prevented truncation of disk store file", e);
      }
    } else if (channel.size() > size) {
      throw new IllegalStateException("Existing file is larger than source limit");
    }
    this.allocator = new PowerOfTwoFileAllocator(size);
  }

  public synchronized Long allocateRegion(long size) {
    Long address = allocator.allocate(size);
    if (address == null) {
      return null;
    }
    allocated.put(address, new AllocatedRegion(address, size));
    /*
     * Ensure the file encompasses fully this region by writing a single byte to
     * its farthest byte before releasing it to the requestor.
     */
    long max = address + size;
    try {
      if (max > channel.size()) {
        ByteBuffer one = ByteBuffer.allocate(1);
        while (one.hasRemaining()) {
          channel.write(one, max - 1);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("IOException while attempting to extend file " + file.getAbsolutePath(), e);
    }
    return address;
  }

  public synchronized void freeRegion(long address) {
    AllocatedRegion r = allocated.remove(address);

    if (r == null) {
      throw new AssertionError();
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Freeing a {}B region from {} &{}", DebuggingUtils.toBase2SuffixedString(r.size), file.getName(), r.address);
      }
      allocator.free(r.address, r.size);
    }
  }

  public synchronized long claimRegion(long address, long size) throws IOException {
    allocator.mark(address, size);
    allocated.put(address, new AllocatedRegion(address, size));
    return address;
  }

  public FileChannel getReadableChannel() {
    try {
      return new RandomAccessFile(file, "r").getChannel();
    } catch (FileNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  public FileChannel getWritableChannel() {
    try {
      return new RandomAccessFile(file, "rw").getChannel();
    } catch (FileNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  public File getFile() {
    return file;
  }

  @Override
  public synchronized MappedPage allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    Long address = allocateRegion(size);
    if (address == null) {
      return null;
    }

    try {
      MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, address, size);
      MappedPage page = new MappedPage(buffer);
      pages.put(page, address);
      return page;
    } catch (IOException e) {
      freeRegion(address);
      LOGGER.warn("Mapping a new file section failed", e);
      return null;
    }
  }

  @Override
  public synchronized void free(final Page page) {
    final Long a = pages.remove(page);

    if (a == null) {
      throw new AssertionError();
    } else {
      /*
       * Do the flush and free asynchronously here since there is no need to
       * do any of this either under the write lock, or immediately.
       */
      ASYNC_FLUSH_EXECUTOR.completeAsynchronously(new Runnable() {
        @Override
        public void run() {
          /*
           * Force all changes out to disk here in case the region is reused and
           * delayed writes from this buffer caused by OS flushing of dirty pages
           * causes corruption of the reused area.
           */
          ((MappedByteBuffer) page.asByteBuffer()).force();
          freeRegion(a);
        }

        @Override
        public String toString() {
          return "Asynchronous flush of Page[" + System.identityHashCode(page) + "] (size=" + page.size() + ")";
        }
      });
    }
  }

  public synchronized MappedPage claimPage(long address, long size) throws IOException {
    claimRegion(address, size);
    MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, address, size);
    MappedPage page = new MappedPage(buffer);
    pages.put(page, address);
    return page;
  }

  public long getAddress(Page underlying) {
    return pages.get(underlying);
  }

  public synchronized void flush() throws IOException {
    if (channel.isOpen()) {
      channel.force(true);
    }
  }

  public synchronized void close() throws IOException {
    try {
      channel.close();
    } finally {
      raf.close();
    }
  }

  static class AllocatedRegion {
    final long address;
    final long size;

    public AllocatedRegion(long address, long size) {
      this.address = address;
      this.size = size;
    }
  }

}
