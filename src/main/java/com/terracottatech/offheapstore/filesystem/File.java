/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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

/**
 * This interface supports both reading and writing to a random access file. A random access file behaves like a large
 * array of bytes stored in the OffHeap file system. There is a kind of cursor, or index into the implied array, called
 * the file pointer; input operations read bytes starting at the file pointer and advance the file pointer past the
 * bytes read. Output operations write bytes starting at the file pointer and advance the file pointer past the bytes
 * written. Output operations that write past the current end of the implied array cause the array to be extended. The
 * file pointer can be read by the <code>getFilePointer</code> method and set by the <code>seek</code> method.
 * 
 * @author dkumar
 */

public interface File {

  /**
   * Reports the length of the file.
   * 
   * @return the length of this file, measured in bytes.
   * @throws IOException if an I/O error occurs
   */
  long length() throws IOException;

  /**
   * Reports the last accessed or modified time for the file.
   * 
   * @return time when the file was last modified.
   * @throws IOException if an I/O error occurs.
   */
  long lastModifiedTime() throws IOException;

  /**
   * Sets the last modified time for the file.
   * 
   * @param time the time to set
   * @throws IOException if an I/O error occurs.
   */
  void setLastModifiedTime(long time) throws IOException;

  /**
   * Opens a stream for reading data from the file
   * 
   * @return a random access input stream.
   * @throws IOException if an error occurs while processing the request.
   */
  SeekableInputStream getInputStream() throws IOException;

  /**
   * Opens a stream for writing data to a file. Only one output stream can be associated with file at any time. 
   * Once an output stream is opened on a file, subsequent calls to this method will return the opened stream's cached reference.
   * 
   * @return a random access output stream for writing data to a file.
   * @throws IOException if an output stream is already open or if an I/O error occurs.
   */
  SeekableOutputStream getOutputStream() throws IOException;

  /**
   * Erases the contents of the file and sets its length to 0
   * 
   * @throws IOException if an I/O error occurs.
   */
  void truncate() throws IOException;

  /**
   * Returns the total size of the file in bytes
   * 
   * @return size in bytes
   */
  long getSizeInBytes();

  /**
   * Returns the name of the file
   * 
   * @return name of the file
   */
  String getName();

}
