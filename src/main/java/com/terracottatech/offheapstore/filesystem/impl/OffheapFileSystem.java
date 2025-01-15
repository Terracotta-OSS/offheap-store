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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.terracottatech.offheapstore.filesystem.Directory;
import com.terracottatech.offheapstore.filesystem.FileSystem;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.util.MemoryUnit;

public class OffheapFileSystem implements FileSystem {

  private final ConcurrentHashMap<String, Directory> directories                = new ConcurrentHashMap<String, Directory>();
  private final PageSource                           pageSource;
  private final int                                  blockSize;
  private final int                                  maxDataPageSize;
  private final int                                  concurrency;
  private static final int                           DEFAULT_BLOCK_SIZE         = MemoryUnit.KILOBYTES.toBytes(8);
  private static final int                           DEFAULT_MAX_DATA_PAGE_SIZE = MemoryUnit.KILOBYTES.toBytes(256);
  private static final int                           DEFAULT_CONCURRENCY        = 4;

  public OffheapFileSystem(PageSource source) {
    this(source, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_DATA_PAGE_SIZE, DEFAULT_CONCURRENCY);
  }

  public OffheapFileSystem(PageSource source, int blockSize, int maxDataPageSize, int concurrency) {
    this.pageSource = source;
    this.blockSize = blockSize;
    this.maxDataPageSize = maxDataPageSize;
    this.concurrency = concurrency;
  }

  @Override
  public synchronized Directory getOrCreateDirectory(String name) throws IOException {
    if (name == null) { throw new NullPointerException("name of the directory is null"); }

    Directory existing = directories.get(name);
    if (existing == null) {
      existing = new OffheapDirectory(name, pageSource, blockSize, maxDataPageSize, concurrency);
      Directory racer = directories.putIfAbsent(name, existing);
      if (racer != null) {
        existing = racer;
      }
    }
    return existing;
  }

  @Override
  public Set<String> listDirectories() throws IOException {
    return Collections.unmodifiableSet(directories.keySet());
  }

  @Override
  public synchronized void deleteDirectory(String name) throws IOException, FileNotFoundException {
    Directory dir = directories.remove(name);
    if (dir == null) throw new FileNotFoundException(name);
    dir.deleteAllFiles();
  }

  @Override
  public void delete() throws IOException {
    for (Iterator<Directory> it = directories.values().iterator(); it.hasNext();) {
      Directory removed = it.next();
      it.remove();
      removed.deleteAllFiles();
    }
  }

  @Override
  public boolean directoryExists(String name) throws IOException {
    return directories.containsKey(name);
  }

}
