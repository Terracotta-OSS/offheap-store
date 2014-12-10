package com.terracottatech.offheapstore.filesystem.impl;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.filesystem.SeekableInputStream;

public class OffheapInputStream extends SeekableInputStream {

  static final ByteBuffer   NULL_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final OffheapFile file;

  private ByteBuffer        currentBlock;
  private int               currentBlockIndex;
  private final int         BLOCK_SIZE;

  OffheapInputStream(OffheapFile file, int blockSize) throws IOException {
    this.BLOCK_SIZE = blockSize;
    this.file = file;
    if (file.length() / BLOCK_SIZE > Integer.MAX_VALUE) { throw new IOException(
                                                                                "OffHeapFileInputStream too large length="
                                                                                    + file.length()); }
    resetBlocks();
  }

  private void resetBlocks() {
    currentBlockIndex = -1;
    currentBlock = NULL_BUFFER;
  }

  @Override
  public void close() throws IOException {
    resetBlocks();
    file.streamClosed(false);
  }

  public long length() throws IOException {
    return file.length();
  }

  @Override
  public int read() throws IOException {
    return ensureCapacity() ? (int) (currentBlock.get() & 0xff) : -1;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      ensureCapacity();
      int lenToRead = Math.min(len, currentBlock.remaining());
      currentBlock.get(b, offset, lenToRead);
      len -= lenToRead;
      offset += lenToRead;
    }
  }

  private boolean ensureCapacity() throws IOException {
    if (!currentBlock.hasRemaining()) {
      if (currentBlockIndex >= file.numBlocks() - 1) { return false; }
      currentBlock = getNextBlockToRead();
    }
    return true;
  }

  private ByteBuffer getNextBlockToRead() throws IOException {
    ByteBuffer buf = file.getBlockToRead(++currentBlockIndex);
    return buf;
  }

  private ByteBuffer getCurrentBlockToRead() throws IOException {
    ByteBuffer buf = file.getBlockToRead(currentBlockIndex);
    return buf;
  }

  @Override
  public long getFilePointer() throws IOException {
    // Convert to long to avoid integer overflow
    return currentBlockIndex < 0 ? 0 : ((long) currentBlockIndex * BLOCK_SIZE) + currentBlock.position();
  }

  @Override
  public void seek(long pos) throws IOException, EOFException {
    if (pos < 0) throw new IOException("Seek position cannot be negative");

    long newIndexPos = pos / BLOCK_SIZE;
    int bufferPosition = (int) (pos % BLOCK_SIZE);

    if (newIndexPos >= file.numBlocks()) {
      // Let it throw EOFException on read;
      resetBlocks();
    } else {
      if (currentBlockIndex != newIndexPos) {
        currentBlockIndex = (int) newIndexPos;
        currentBlock = getCurrentBlockToRead();
      }
      // move within the current buffer
      if (bufferPosition > currentBlock.limit()) { throw new EOFException("seek past EOF"); }
      currentBlock.position(bufferPosition);
    }
  }

  @Override
  public String toString() {
    return "OffHeapFileInputStream@" + System.identityHashCode(this);
  }

}