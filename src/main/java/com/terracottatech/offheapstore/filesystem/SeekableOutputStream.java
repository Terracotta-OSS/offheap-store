/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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
package com.terracottatech.offheapstore.filesystem;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This interface provides for writing bytes to an OffHeap file. For all the methods in this interface that write bytes,
 * it is generally true that if a byte cannot be written for any reason, an IOException is thrown.
 * 
 * @author dkumar
 */
public abstract class SeekableOutputStream extends OutputStream {

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract void write(int b) throws IOException;

  /**
   * Returns the current offset in this file.
   * 
   * @return the offset from the beginning of the file, in bytes, at which the next read occurs.
   * @throws IOException if an I/O error occurs.
   */
  public abstract long getFilePointer() throws IOException;

  /**
   * Sets the file-pointer offset, measured from the beginning of this file, at which the next write occurs. The offset
   * may be set beyond the end of the file. Setting the offset beyond the end of the file does not change the file
   * length. The file length will change only by writing after the offset has been set beyond the end of the file.
   * 
   * @param pos - the offset position, measured in bytes from the beginning of the file, at which to set the file
   *        pointer.
   * @throws IOException if <code>pos</code> is less than 0 or if an I/O error occurs.
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Clears the stream. Calling <code>flush</code> after a <code>reset</code> will clear the contents of the file.
   * 
   * @throws IOException
   */
  public abstract void reset() throws IOException;
  
  /**
   * Returns the length of the stream. This method flushes the buffers to the file before calculating the length.
   * 
   * @return length of the stream in bytes
   * @throws IOException
   */
  public abstract long length() throws IOException;

}
