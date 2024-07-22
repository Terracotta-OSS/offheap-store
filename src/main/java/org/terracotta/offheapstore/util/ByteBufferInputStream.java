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
package org.terracotta.offheapstore.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

/**
 *
 * @author Chris Dennis
 */
public class ByteBufferInputStream extends InputStream {

  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer.hasRemaining()) {
      return buffer.get() & 0xff;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int size = Math.min(len, buffer.remaining());
    if (size <= 0) {
      return -1;
    }

    buffer.get(b, off, size);

    return size;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n < 0) {
      return 0;
    } else {
      long skip = Math.min(n, buffer.remaining());
      buffer.position((int) (buffer.position() + skip));
      return skip;
    }
  }

  @Override
  public int available() throws IOException {
    return buffer.remaining();
  }

  @Override
  public synchronized void mark(int readlimit) {
    buffer.mark();
  }

  @Override
  public synchronized void reset() throws IOException {
    try {
      buffer.reset();
    } catch (InvalidMarkException e) {
      throw (IOException) (new IOException().initCause(e));
    }
  }

  @Override
  public boolean markSupported() {
    return true;
  }
}
