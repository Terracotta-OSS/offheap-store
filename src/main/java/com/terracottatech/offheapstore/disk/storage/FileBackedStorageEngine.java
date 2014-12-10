/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.persistent.Persistent;
import com.terracottatech.offheapstore.disk.persistent.PersistentStorageEngine;
import com.terracottatech.offheapstore.storage.PortabilityBasedStorageEngine;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.WriteContext;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class FileBackedStorageEngine<K, V> extends PortabilityBasedStorageEngine<K, V> implements PersistentStorageEngine<K, V> {

  private static final int MAGIC = 0x414d4445;
  private static final int MAGIC_CHUNK = 0x4e4e4953;

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBackedStorageEngine.class);

  private static final int KEY_HASH_OFFSET = 0;
  private static final int KEY_LENGTH_OFFSET = 4;
  private static final int VALUE_LENGTH_OFFSET = 8;
  private static final int KEY_DATA_OFFSET = 12;

  private final ConcurrentHashMap<Long, FileWriteTask> pendingWrites = new ConcurrentHashMap<Long, FileWriteTask>();
  private final ExecutorService writeExecutor;

  private final MappedPageSource source;

  private final FileChannel writeChannel;
  private final AtomicReference<FileChannel> readChannelReference;

  private final long initialSize;
  private final int chunkIndexOffset;
  private final List<FileChunk> chunks = new CopyOnWriteArrayList<FileChunk>();

  private volatile Owner owner;

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final int initialSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability) {
    return createFactory(source, initialSize, keyPortability, valuePortability, true);
  }

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final int initialSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean bootstrap) {
    Factory<ThreadPoolExecutor> executorFactory = new Factory<ThreadPoolExecutor>() {
      @Override
      public ThreadPoolExecutor newInstance() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
      }
    };
    return createFactory(source, initialSize, keyPortability, valuePortability, executorFactory, bootstrap);
  }

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final int initialSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final Factory<ThreadPoolExecutor> executorFactory, final boolean bootstrap) {
    return new Factory<FileBackedStorageEngine<K, V>>() {
      @Override
      public FileBackedStorageEngine<K, V> newInstance() {
        return new FileBackedStorageEngine<K, V>(source, keyPortability, valuePortability, initialSize, executorFactory.newInstance(), bootstrap);
      }
    };
  }

  public FileBackedStorageEngine(MappedPageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, int initialSize) {
    this(source, keyPortability, valuePortability, initialSize, true);
  }

  public FileBackedStorageEngine(MappedPageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, int initialSize, ThreadPoolExecutor writer) {
    this(source, keyPortability, valuePortability, initialSize, writer, true);
  }

  public FileBackedStorageEngine(MappedPageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, int initialSize, boolean bootstrap) {
    this(source, keyPortability, valuePortability, initialSize, new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()), bootstrap);
  }

  public FileBackedStorageEngine(MappedPageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, int initialSize, ThreadPoolExecutor writer, boolean bootstrap) {
    super(keyPortability, valuePortability);

    if (writer.getMaximumPoolSize() > 1) {
      throw new AssertionError();
    } else {
      this.writeExecutor = writer;
    }

    this.writeChannel = source.getWritableChannel();
    this.readChannelReference = new AtomicReference<FileChannel>(source.getReadableChannel());

    this.source = source;
    this.initialSize = initialSize;
    this.chunkIndexOffset = 32 - Integer.numberOfLeadingZeros(initialSize);

    if (bootstrap) {
      this.chunks.add(new FileChunk(initialSize, 0));
    }
  }

  @Override
  protected void clearInternal() {
    for (FileChunk c : chunks) {
      c.clear();
      if (!chunks.remove(c)) {
        throw new AssertionError("Concurrent modification while clearing!");
      }
    }
    if (!chunks.isEmpty()) {
      throw new AssertionError("Concurrent modification while clearing!");
    }
    this.chunks.add(new FileChunk(initialSize, 0));
  }

  @Override
  public void destroy() {
    try {
      close();
    } catch (IOException e) {
      LOGGER.warn("Exception while trying to close file backed storage engine", e);
    }
  }

  @Override
  public void flush() throws IOException {
    Future<Void> flush = writeExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        writeChannel.force(true);
        return null;
      }
    });

    boolean interrupted = false;
    try {
      while (true) {
        try {
          flush.get();
          break;
        } catch (InterruptedException ex) {
          interrupted = true;
        } catch (ExecutionException ex) {
          Throwable cause = ex.getCause();
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else if (cause instanceof IOException) {
            throw (IOException) cause;
          } else {
            throw new RuntimeException(cause);
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
  public void close() throws IOException {
    try {
      writeExecutor.shutdownNow();
      if (writeExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        LOGGER.debug("FileBackedStorageEngine for " + source.getFile().getName() + " terminated successfully");
      } else {
        LOGGER.warn("FileBackedStorageEngine for " + source.getFile().getName() + " timed-out during termination");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("FileBackedStorageEngine for " + source.getFile().getName() + " interrupted during termination");
      Thread.currentThread().interrupt();
    } finally {
      try {
        writeChannel.close();
      } finally {
        readChannelReference.getAndSet(null).close();
      }
    }
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    output.writeInt(MAGIC);
    ((Persistent) keyPortability).persist(output);
    ((Persistent) valuePortability).persist(output);
    output.writeInt(chunks.size());
    for (FileChunk c : chunks) {
      c.persist(output);
    }
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    if (!chunks.isEmpty()) {
      throw new IllegalStateException();
    }
    if (input.readInt() != MAGIC) {
      throw new IOException("Wrong magic number");
    }

    ((Persistent) keyPortability).bootstrap(input);
    ((Persistent) valuePortability).bootstrap(input);

    int n = input.readInt();
    for (int i = 0; i < n; i++) {
      chunks.add(new FileChunk(input));
    }

    if (hasRecoveryListeners()) {
      // XXX: At some point I think this behavior will need to be optional
      // In particular when we support re-using on-disk lucene search indexes
      // and we know we have a clean shutdown we don't want to do any of this
      // deserialization
      // XXX: _perhaps_ we could use a method or marker interface on the listener
      // to make this decision
      for (Long encoding : owner.encodingSet()) {
        ByteBuffer binaryKey = readBinaryKey(encoding);
        ByteBuffer binaryValue = readBinaryValue(encoding);
        int hash = readKeyHash(encoding);

        final ByteBuffer binaryKeyForDecode = binaryKey.duplicate();
        final ByteBuffer binaryValueForDecode = binaryValue.duplicate();
        final Thread caller = Thread.currentThread();
        fireRecovered(new Callable<K>() {

          @Override
          public K call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (K) keyPortability.decode(binaryKeyForDecode.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, new Callable<V>() {

          @Override
          public V call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (V) valuePortability.decode(binaryValueForDecode.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, binaryKey, binaryValue, hash, 0, encoding);
      }
    }
  }

  @Override
  protected void free(long address) {
    FileChunk chunk = findChunk(address);
    chunk.free(address - chunk.baseAddress());
  }

  @Override
  protected ByteBuffer readKeyBuffer(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readKeyBuffer(address - chunk.baseAddress());
  }

  @Override
  protected WriteContext getKeyWriteContext(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.getKeyWriteContext(address - chunk.baseAddress());
  }

  @Override
  protected ByteBuffer readValueBuffer(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readValueBuffer(address - chunk.baseAddress());
  }

  @Override
  protected WriteContext getValueWriteContext(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.getValueWriteContext(address - chunk.baseAddress());
  }

  @Override
  protected Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int hash) {
    for (FileChunk c : chunks) {
      Long address = c.writeMappingBuffers(keyBuffer, valueBuffer, hash);
      if (address != null) {
        return address + c.baseAddress();
      }
    }

    while (true) {
      FileChunk last = chunks.get(chunks.size() - 1);
      long nextChunkSize = last.capacity() << 1;
      long nextChunkBaseAddress = last.baseAddress() + last.capacity();

      if (nextChunkSize < 0) {
        return null;
      }

      FileChunk c;
      try {
        c = new FileChunk(nextChunkSize, nextChunkBaseAddress);
      } catch (OutOfMemoryError e) {
        return null;
      }
      chunks.add(c);
      Long address = c.writeMappingBuffers(keyBuffer, valueBuffer, hash);
      if (address != null) {
        return address + c.baseAddress();
      }
    }
  }

  @Override
  public long getAllocatedMemory() {
    long sum = 0;
    for (FileChunk c : chunks) {
      sum += c.capacity();
    }
    return sum;
  }

  @Override
  public long getOccupiedMemory() {
    long sum = 0;
    for (FileChunk c : chunks) {
      sum += c.occupied();
    }
    return sum;
  }

  @Override
  public long getVitalMemory() {
    return getAllocatedMemory();
  }

  @Override
  public long getDataSize() {
    long sum = 0;
    for (FileChunk c : chunks) {
      sum += c.occupied();
    }
    return sum;
  }
  
  private FileChunk findChunk(long address) {
    int chunkIndex = Long.SIZE - Long.numberOfLeadingZeros(address + initialSize) - chunkIndexOffset;
    return chunks.get(chunkIndex);
  }

  private int readIntFromChannel(long position) throws IOException {
    ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    for (int i = 0; lengthBuffer.hasRemaining(); ) {
      int read = readFromChannel(lengthBuffer, position + i);
      if (read < 0) {
        throw new EOFException();
      } else {
        i += read;
      }
    }
    return ((ByteBuffer) lengthBuffer.flip()).getInt();
  }

  private void writeIntToChannel(long position, int data) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    buffer.putInt(data).flip();
    writeBufferToChannel(position, buffer);
  }

  private void writeLongToChannel(long position, long data) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
    buffer.putLong(data).flip();
    writeBufferToChannel(position, buffer);
  }

  private void writeBufferToChannel(long position, ByteBuffer buffer) throws IOException {
    for (int i = 0; buffer.hasRemaining(); ) {
      int written = writeChannel.write(buffer, position + i);
      if (written < 0) {
        throw new EOFException();
      } else {
        i += written;
      }
    }
  }

  private int readFromChannel(ByteBuffer buffer, long position) throws IOException {
    FileChannel current = readChannelReference.get();
    if (current == null) {
      throw new IOException("Storage engine is closed");
    } else {
      try {
        return readFromChannel(current, buffer, position);
      } catch (ClosedChannelException e) {
        boolean interrupted = Thread.interrupted();
        try {
          while (true) {
            current = readChannelReference.get();
            try {
              return readFromChannel(current, buffer, position);
            } catch (ClosedChannelException f) {
              interrupted |= Thread.interrupted();

              FileChannel newChannel = source.getReadableChannel();
              if (!readChannelReference.compareAndSet(current, newChannel)) {
                newChannel.close();
              } else {
                LOGGER.info("Creating new read-channel for " + source.getFile().getName() + " as previous one was closed (likely due to interrupt)");
              }
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  private int readFromChannel(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
    int ret = channel.read(buffer, position);
    if (ret < 0) {
      ret = channel.read(buffer, position);
    }
    return ret;
  }

  @Override
  public boolean shrink() {
    //TODO May want to support shrink on disk...
    return false;
  }

  @Override
  public void bind(Owner m) {
    if (owner != null) {
      throw new AssertionError();
    }
    owner = m;
  }

  class FileChunk {
    private final AATreeFileAllocator allocator;
    private final long filePosition;
    private final long baseAddress;

    private boolean valid = true;

    FileChunk(long size, long baseAddress) {
      Long newOffset = source.allocateRegion(size);
      if (newOffset == null) {
        StringBuilder sb = new StringBuilder("Storage engine file data area allocation failed:\n");
        sb.append("Allocator: ").append(source);
        throw new OutOfMemoryError(sb.toString());
      } else {
        this.filePosition = newOffset;
      }
      this.allocator = new AATreeFileAllocator(size);
      this.baseAddress = baseAddress;
    }

    FileChunk(ObjectInput input) throws IOException {
      if (input.readInt() != MAGIC_CHUNK) {
        throw new IOException("Wrong magic number");
      }
      this.filePosition = input.readLong();
      this.baseAddress = input.readLong();

      long size = input.readLong();
      source.claimRegion(filePosition, size);

      this.allocator = new AATreeFileAllocator(size, input);
    }

    ByteBuffer readKeyBuffer(long address) {
      try {
        long position = filePosition + address;
        FileWriteTask pending = pendingWrites.get(position);
        if (pending == null) {
          int keyLength = readIntFromChannel(position + KEY_LENGTH_OFFSET);
          return readBuffer(position + KEY_DATA_OFFSET, keyLength);
        } else {
          return pending.getKeyBuffer();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (OutOfMemoryError e) {
        LOGGER.error("Failed to allocate direct buffer for FileChannel read.  "
                + "Consider increasing the -XX:MaxDirectMemorySize property to "
                + "allow enough space for the FileChannel transfer buffers");
        throw e;
      }
    }

    protected int readPojoHash(long address) {
      try {
        long position = filePosition + address;
        FileWriteTask pending = pendingWrites.get(position);
        if (pending == null) {
          return readIntFromChannel(position + KEY_HASH_OFFSET);
        } else {
          return pending.pojoHash;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (OutOfMemoryError e) {
        LOGGER.error("Failed to allocate direct buffer for FileChannel read.  "
                + "Consider increasing the -XX:MaxDirectMemorySize property to "
                + "allow enough space for the FileChannel transfer buffers");
        throw e;
      }
    }

    ByteBuffer readValueBuffer(long address) {
      try {
        long position = filePosition + address;
        FileWriteTask pending = pendingWrites.get(position);
        if (pending == null) {
          int keyLength = readIntFromChannel(position + KEY_LENGTH_OFFSET);
          int valueLength = readIntFromChannel(position + VALUE_LENGTH_OFFSET);
          return readBuffer(position + keyLength + KEY_DATA_OFFSET, valueLength);
        } else {
          return pending.getValueBuffer();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (OutOfMemoryError e) {
        LOGGER.error("Failed to allocate direct buffer for FileChannel read.  "
                + "Consider increasing the -XX:MaxDirectMemorySize property to "
                + "allow enough space for the FileChannel transfer buffers");
        throw e;
      }
    }

    ByteBuffer readBuffer(long position, int length) {
      try {
        ByteBuffer data = ByteBuffer.allocate(length);
        for (int i = 0; data.hasRemaining(); ) {
          int read = readFromChannel(data, position + i);
          if (read < 0) {
            throw new EOFException();
          } else {
            i += read;
          }
        }
        return (ByteBuffer) data.rewind();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (OutOfMemoryError e) {
        LOGGER.error("Failed to allocate direct buffer for FileChannel read.  "
                + "Consider increasing the -XX:MaxDirectMemorySize property to "
                + "allow enough space for the FileChannel transfer buffers");
        throw e;
      }
    }

    Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int pojoHash) {
      int keyLength = keyBuffer.remaining();
      int valueLength = valueBuffer.remaining();
      long address = allocator.allocate(keyLength + valueLength + KEY_DATA_OFFSET);

      if (address >= 0) {
        long position = filePosition + address;
        FileWriteTask task = new FileWriteTask(this, position, keyBuffer, valueBuffer, pojoHash);
        pendingWrites.put(position, task);
        writeExecutor.execute(task);
        return address;
      } else {
        return null;
      }
    }

    WriteContext getKeyWriteContext(long address) {
      try {
        long position = filePosition + address;
        FileWriteTask pending = pendingWrites.get(position);
        if (pending == null) {
          int keyLength = readIntFromChannel(position + KEY_LENGTH_OFFSET);
          return getDiskWriteContext(position + KEY_DATA_OFFSET, keyLength);
        } else {
          if (pendingWrites.get(position) != pending) {
            return getKeyWriteContext(address);
          } else {
            return getQueuedWriteContext(pending, pending.getKeyBuffer());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    WriteContext getValueWriteContext(long address) {
      try {
        long position = filePosition + address;
        FileWriteTask pending = pendingWrites.get(position);
        if (pending == null) {
          int keyLength = readIntFromChannel(position + KEY_LENGTH_OFFSET);
          int valueLength = readIntFromChannel(position + VALUE_LENGTH_OFFSET);
          return getDiskWriteContext(position + keyLength + KEY_DATA_OFFSET, valueLength);
        } else {
          if (pendingWrites.get(position) != pending) {
            return getValueWriteContext(address);
          } else {
            return getQueuedWriteContext(pending, pending.getValueBuffer());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private WriteContext getDiskWriteContext(final long address, final int max) {
      return new WriteContext() {
        @Override
        public void setLong(int offset, long value) {
          if (offset < 0 || offset >= max) {
            throw new IllegalArgumentException();
          } else {
            try {
              writeLongToChannel(address + offset, value);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }

        @Override
        public void flush() {
          //no-op
        }
      };
    }

    private WriteContext getQueuedWriteContext(final FileWriteTask current, final ByteBuffer queuedBuffer) {
      return new WriteContext() {
        @Override
        public void setLong(int offset, long value) {
          queuedBuffer.putLong(offset, value);
        }

        @Override
        public void flush() {
          FileWriteTask flush = new FileWriteTask(current.chunk, current.position, current.keyBuffer, current.valueBuffer, current.pojoHash);
          pendingWrites.put(flush.position, flush);
          writeExecutor.execute(flush);
        }
      };
    }

    void free(long address) {
      try {
        long position = filePosition + address;
        int keyLength;
        int valueLength;
        FileWriteTask pending = pendingWrites.remove(position);
        if (pending == null) {
          keyLength = readIntFromChannel(position + KEY_LENGTH_OFFSET);
          valueLength = readIntFromChannel(position + VALUE_LENGTH_OFFSET);
        } else {
          keyLength = pending.getKeyBuffer().remaining();
          valueLength = pending.getValueBuffer().remaining();
        }
        allocator.free(address, keyLength + valueLength + KEY_DATA_OFFSET);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    synchronized void clear() {
      source.freeRegion(filePosition);
      valid = false;
    }

    long capacity() {
      return allocator.capacity();
    }

    long occupied() {
      return allocator.occupied();
    }

    long baseAddress() {
      return baseAddress;
    }

    void persist(ObjectOutput output) throws IOException {
      output.writeInt(MAGIC_CHUNK);
      output.writeLong(filePosition);
      output.writeLong(baseAddress);
      output.writeLong(allocator.capacity());
      allocator.persist(output);
    }

    synchronized boolean isValid() {
      return valid;
    }
  }

  class FileWriteTask implements Runnable {

    private final FileChunk chunk;
    private final ByteBuffer keyBuffer;
    private final ByteBuffer valueBuffer;
    private final int pojoHash;
    private final long position;

    FileWriteTask(FileChunk chunk, long position, ByteBuffer keyBuffer, ByteBuffer valueBuffer, int pojoHash) {
      this.chunk = chunk;
      this.position = position;
      this.keyBuffer = keyBuffer;
      this.valueBuffer = valueBuffer;
      this.pojoHash = pojoHash;
    }

    @Override
    public void run() {
      if (pendingWrites.get(Long.valueOf(position)) != this) {
        return;
      } else {
        try {
          synchronized (chunk) {
            if (chunk.isValid()) {
              try {
                try {
                  write();
                } catch (IOException e) {
                  LOGGER.warn("Received IOException '{}' while trying to write @ {} : trying again", e.getMessage(), position);
                  write();
                }
              } catch (ClosedChannelException e) {
                LOGGER.debug("DiskWriteTask terminated due to closed channel - we must be shutting down", e);
              } catch (IOException e) {
                LOGGER.warn("Received IOException '{}' during write @ {} : giving up", e.getMessage(), position);
              } catch (OutOfMemoryError e) {
                LOGGER.error("Failed to allocate a direct buffer for a FileChannel write.  "
                        + "Consider increasing the -XX:MaxDirectMemorySize property to "
                        + "allow enough space for the FileChannel transfer buffers");
                throw e;
              }
            }
          }
        } finally {
          pendingWrites.remove(Long.valueOf(position), this);
        }
      }
    }

    private void write() throws IOException {
      ByteBuffer key = getKeyBuffer();
      ByteBuffer value = getValueBuffer();
      int keyLength = key.remaining();
      int valueLength = value.remaining();

      writeIntToChannel(position + KEY_HASH_OFFSET, pojoHash);
      writeIntToChannel(position + KEY_LENGTH_OFFSET, keyLength);
      writeIntToChannel(position + VALUE_LENGTH_OFFSET, valueLength);
      writeBufferToChannel(position + KEY_DATA_OFFSET, key);
      writeBufferToChannel(position + KEY_DATA_OFFSET + keyLength, value);

      long size = writeChannel.size();
      long expected = position + keyLength + valueLength + KEY_DATA_OFFSET;
      if (size < expected) {
        throw new IOException("File size does not encompass last write [size:" + size + " end-of-write:" + expected);
      }
    }

    ByteBuffer getKeyBuffer() {
      return keyBuffer.duplicate();
    }

    ByteBuffer getValueBuffer() {
      return valueBuffer.duplicate();
    }
  }

  @Override
  public int readKeyHash(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readPojoHash(address - chunk.baseAddress());
  }
}
