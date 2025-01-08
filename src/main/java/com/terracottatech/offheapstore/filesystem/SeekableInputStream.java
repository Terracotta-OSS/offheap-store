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
package com.terracottatech.offheapstore.filesystem;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * <p>
 * This interface provides for reading bytes from an OffHeap File.
 * <p>
 * It is generally true of all the reading routines in this interface that if end of file is reached before the desired
 * number of bytes has been read, an EOFException (which is a kind of IOException) is thrown. If any byte cannot be read
 * for any reason other than end of file, an IOException other than EOFException is thrown. In particular, an
 * IOException may be thrown if the input stream has been closed.
 * 
 * @author dkumar
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int read() throws IOException;
  
  /**
   * {@inheritDoc}
   */
  public abstract void readBytes(byte[] b, int offset, int len) throws IOException;  

  /**
   * Returns the current offset in this file.
   * 
   * @return the offset from the beginning of the file, in bytes, at which the next read occurs.
   * @throws IOException if an I/O error occurs.
   */
  public abstract long getFilePointer() throws IOException;

  /**
   * <p>
   * Closes this stream and releases any system resources associated with the stream.
   * 
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * Sets the file-pointer offset, measured from the beginning of this file, at which the next read occurs.
   * 
   * @param pos - the offset position, measured in bytes from the beginning of the file, at which to set the file
   *        pointer.
   * @throws IOException if <code>pos</code> is less than 0 or if an I/O error occurs.
   * @throws EOFException if <code>pos</code> beyond end of file
   */
  public abstract void seek(long pos) throws IOException, EOFException; 
  
}
