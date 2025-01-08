/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package com.terracottatech.offheapstore.filesystem.impl;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.filesystem.File;
import com.terracottatech.offheapstore.filesystem.SeekableInputStream;
import com.terracottatech.offheapstore.filesystem.SeekableOutputStream;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

public class OffheapFile implements File {

  private static final boolean VALIDATING = shouldValidate(OffheapFile.class);
  
  private final PageSource               source;
  private final String                   name;
  private final Map<Integer, ByteBuffer> blockMap;
  private final AtomicInteger            blockCount       = new AtomicInteger(0);
  private volatile long                  length;
  private volatile long                  lastModified  = System.currentTimeMillis();

  // memory is assigned to a file in discrete blocks equal to BLOCK_SIZE
  // file starts off with one BLOCK_SIZE total size and more blocks are added as needed
  private final int                      blockSize;

  // max data page size is the maximum size of a page for the OffHeap's memory allocator
  private final int                      maxDataPageSize;
  private static final int               TABLE_SIZE    = 128;
  private final int                      concurrency;
  private OffheapOutputStream            outputStream;
  private final AtomicInteger            inputStreamCount = new AtomicInteger(0);
  private volatile boolean               pendingDelete = false;
  private final ByteBuffer               outBuffer;
  
  private static final Logger LOGGER      = LoggerFactory.getLogger(OffheapFile.class);
  private final boolean       debug       = LOGGER.isDebugEnabled();

  OffheapFile(OffheapDirectory dir, String fileName, PageSource source, int blockSize, int maxDataPageSize,
              int concurrency) {
    this.name = fileName;
    this.source = source;
    this.blockSize = blockSize;
    this.maxDataPageSize = maxDataPageSize;
    this.concurrency = concurrency;
    this.blockMap = createOffHeapMap();
    this.outBuffer = newBlock(blockSize);
    if (debug) {
      LOGGER.debug("Creating Offheap File: " + this);
    }
  }

  private ConcurrentMap<Integer, ByteBuffer> createOffHeapMap() {
    /*
     * Creates a map using the given table buffer source, storage engine factory, initial table size, and concurrency.
     * tableSource - buffer source from which hash tables are allocated storageEngineFactory - factory for the segment
     * storage engines tableSize - initial table size (summed across all segments) concurrency - number of segments
     */

    Factory<SplitStorageEngine<Integer, ByteBuffer>> storageEngineFactory = SplitStorageEngine
        .<Integer, ByteBuffer> createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine
            .<ByteBuffer> createFactory(source, maxDataPageSize, maxDataPageSize, new ValuePortability(), false, false));
    return new ConcurrentOffHeapHashMap<Integer, ByteBuffer>(source, true, storageEngineFactory, TABLE_SIZE,
                                                             concurrency);
  }

  @Override
  public long length() throws IOException {
    return length;
  }

  @Override
  public long lastModifiedTime() throws IOException {
    return lastModified;
  }

  @Override
  public void setLastModifiedTime(long time) throws IOException {
    lastModified = time;
  }

  @Override
  public SeekableInputStream getInputStream() throws IOException {
    inputStreamCount.incrementAndGet();
    OffheapInputStream stream = new OffheapInputStream(this, blockSize);

    return stream;
  }

  @Override
  public synchronized SeekableOutputStream getOutputStream() throws IOException {
    if (outputStream == null) {
      outputStream = new OffheapOutputStream(this, blockSize);
      return outputStream;
    } else {
      return outputStream;
    }
  }

  synchronized void streamClosed(boolean isOut) throws IOException {
    if (isOut) outputStream = null;
    else inputStreamCount.decrementAndGet();
    if (pendingDelete && okToDelete()) doDelete();
  }

  @Override
  public String toString() {
    return "Name: " + name + "\nBlock Size: " + blockSize + "\nMax Data Page Size: " + maxDataPageSize + "\nConcurrency: " + concurrency + "\n\n";
  }

  @Override
  public String getName() {
    return name;
  }

  synchronized void setLength(long newLength) {
    validateLength(newLength);
    this.length = newLength;
    for (int i = blockCount.get(); i > (int) (newLength / blockSize); i--) {
      blockMap.remove(i);
      blockCount.decrementAndGet();
    }
  }

  private void validateLength(long newLength) {
    if (newLength < 0) { throw new AssertionError("Length cant be negative : current : " + length + " new : "
                                                  + newLength); }
    if (newLength / blockSize > Integer.MAX_VALUE) { throw new AssertionError("File Length too large : Block size : "
                                                                              + blockSize + " file size : " + newLength); }
  }

  private void setLengthOnFlush(long newLength) {
    validateLength(newLength);
    if (length < newLength) {
      length = newLength;
    }
  }

  int numBlocks() {
    return blockMap.size();
  }

  private ByteBuffer newBlock(int size) {
    return ByteBuffer.allocate(size);
  }

  private final ByteBuffer addBlock(int size) {
    blockCount.getAndIncrement();
    outBuffer.clear();
    return outBuffer;
  }

  private final ByteBuffer getBlock(int index) {
    ByteBuffer ret = null;
    while (index >= blockCount.get()) {
      ret = addBlock(blockSize);
    }
    if (ret != null) {
      return ret;
    } else {
      ret = blockMap.get(index);
      return ret;
    }
  }

  // Returns buffer at index. Adds new empty buffers as needed. The returned buffers are primed for writing.
  final ByteBuffer getBlockToWrite(int index) {
    ByteBuffer buf = getBlock(index);
    ByteBuffer copy = newBlock(buf.capacity());
    copy.put(buf);
    copy.flip();
    return copy;
  }

  // Returns buffer at index for reading
  final ByteBuffer getBlockToRead(int index) throws IOException {
    // in case if file has been just created or it has been closed
    if (blockMap.size() == 0) throw new IOException("File is either empty or it has been deleted");
    int lastIndex = blockMap.size() - 1;
    if (lastIndex < index) throw new EOFException("Attempt to read past end : lastIndex= " + lastIndex + " index = "
                                                  + index);
    ByteBuffer buf = getBlock(index).asReadOnlyBuffer();
    buf.position(0);
    int bufEnd = (int) (length() % blockSize);
    if (index == lastIndex && bufEnd != 0) {
      buf.limit(bufEnd);
    } else {
      buf.limit(blockSize);
    }
    return buf;
  }

  @Override
  public long getSizeInBytes() {
    return blockCount.get() * blockSize;
  }

  // Flush buffers into OffHeap. This methods makes sure it doesn't mutate the underlying buffer in anyway.
  void flush(int index, ByteBuffer buffer, int bufferEnd) throws IOException {
    if (index + 1 == blockCount.get()) {
      long newLength = ((long) index * blockSize) + bufferEnd;
      setLengthOnFlush(newLength);
    }
    blockMap.put(index, buffer);
    setLastModifiedTime(System.currentTimeMillis());
  }

  synchronized void delete() throws IOException {
    if (!okToDelete()) {
      pendingDelete = true;
    } else {
      doDelete();
    }
  }

  private void doDelete() {
    validate(!VALIDATING || Thread.holdsLock(this));
    blockMap.clear();
    blockCount.set(0);
    pendingDelete = false;
  }

  private synchronized boolean okToDelete() {
    return !(anyInputStreamsAreOpen() || outputStreamIsOpen());
  }

  boolean anyInputStreamsAreOpen() {
    validate(!VALIDATING || Thread.holdsLock(this));
    return inputStreamCount.get() != 0;
  }

  boolean outputStreamIsOpen() {
    return outputStream != null;
  }

  @Override
  public synchronized void truncate() throws IOException {
    if (length == 0) return;
    if (outputStream != null) {
      // reset & flush free the resources and they set the length to 0
      outputStream.reset();
      blockCount.set(0);
      blockMap.clear();
    } else {
      blockCount.set(0);
      blockMap.clear();
    }
  }
  

  public static class ValuePortability implements Portability<ByteBuffer> {
    @Override
    public ByteBuffer decode(ByteBuffer src) {
      return src;
    }

    @Override
    public ByteBuffer encode(ByteBuffer buf) {
      ByteBuffer buffer = (buf).asReadOnlyBuffer();
      buffer.position(0);
      buffer.limit(buffer.capacity());
      return buffer;
    }

    @Override
    public boolean equals(Object value, ByteBuffer buf) {
      return value.equals(buf);
    }
  }
}
