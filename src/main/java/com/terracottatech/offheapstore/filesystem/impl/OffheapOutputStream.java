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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.filesystem.SeekableOutputStream;

/**
 * <p>
 * Each file is made up of multiple blocks of size <code>BLOCK_SIZE</code>. There are two pointers to track the writes:
 * <ol>
 * <li>The <code>currentBufferIndex</code> tracks the block number which should be written to.
 * <li>The <code>currentBuffer</code> is the actual block to which the bytes are written. It is a simple
 * <code>ByteBuffer</code>.
 * </ol>
 * <p>
 * The actual position at which the next byte is written, from the beginning of the file is:
 * <code>(currentBufferIndex * BLOCK_SIZE) + currentBuffer.position()</code>
 */

public class OffheapOutputStream extends SeekableOutputStream {

  private final int           BLOCK_SIZE;
  static final ByteBuffer     NULL_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final OffheapFile   file;

  // each buffer can hold up to BLOCK_SIZE bytes
  private ByteBuffer          currentBuffer;

  private int                 currentBufferIndex;

  // Last written position in the current buffer, not necessarily the EOF
  private int                 bufferEnd;


  OffheapOutputStream(OffheapFile file, int blockSize) {
    this.BLOCK_SIZE = blockSize;
    this.file = file;
    this.currentBufferIndex = -1;
    this.currentBuffer = NULL_BUFFER;
    this.bufferEnd = -1;
  }

  @Override
  public void write(int b) throws IOException {
    ensureCapacity();
    currentBuffer.put((byte) b);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      ensureCapacity();
      int lenToWrite = Math.min(len, currentBuffer.remaining());
      currentBuffer.put(b, offset, lenToWrite);
      len -= lenToWrite;
      offset += lenToWrite;
    }
  }

  private void ensureCapacity() throws IOException {
    if (!currentBuffer.hasRemaining()) {
      flush();
      currentBuffer = getNextBufferToWrite();
    }
  }

  private ByteBuffer getNextBufferToWrite() {
    bufferEnd = -1;
    if (++currentBufferIndex < 0) {
      // Overflow
      throw new AssertionError("File size too large to handle : " + currentBufferIndex);
    }
    return file.getBlockToWrite(currentBufferIndex);
  }

  /** Resets this to an empty file. */
  @Override
  public void reset() throws IOException {
    currentBuffer = NULL_BUFFER;
    currentBufferIndex = -1;
    bufferEnd = -1;
    file.setLength(0);
  }

  @Override
  public void close() throws IOException {
    flush();
    file.streamClosed(true);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) throw new IOException("Seek position cannot be negative");
    long newIndexPos = pos / BLOCK_SIZE;
    int bufferPosition = (int) (pos % BLOCK_SIZE);
    if (newIndexPos > Integer.MAX_VALUE) { throw new AssertionError("Seek position too large to handle : " + pos
                                                                    + " block size : " + BLOCK_SIZE); }

    flush();
    if (currentBufferIndex != newIndexPos) {
      currentBufferIndex = (int) newIndexPos;
      currentBuffer = file.getBlockToWrite(currentBufferIndex);
      bufferEnd = -1;
    } else {
      // Mark last written position
      bufferEnd = Math.max(bufferEnd, currentBuffer.position());
    }
    // move within the current buffer
    currentBuffer.position(bufferPosition);
  }

  @Override
  public String toString() {
    return "Current Buffer : " + currentBuffer + " index : " + currentBufferIndex + " bufferEnd : " + bufferEnd;
  }

  @Override
  public long length() throws IOException {
    flush();
    return file.length();
  }

  @Override
  public void flush() throws IOException {
    if (currentBuffer == NULL_BUFFER) { return; }
    if (bufferEnd != -1 && currentBuffer.position() < bufferEnd) {
      file.flush(currentBufferIndex, currentBuffer, bufferEnd);
    } else {
      file.flush(currentBufferIndex, currentBuffer, currentBuffer.position());
    }
    bufferEnd = -1;
  }

  @Override
  public long getFilePointer() {
    // Convert to long to avoid integer overflow
    return currentBufferIndex < 0 ? 0 : ((long) currentBufferIndex * BLOCK_SIZE) + currentBuffer.position();
  }

  /** Returns byte usage of all buffers. */
  public long sizeInBytes() {
    return file.getSizeInBytes();
  }

}
