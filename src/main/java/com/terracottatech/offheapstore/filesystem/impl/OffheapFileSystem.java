package com.terracottatech.offheapstore.filesystem.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.terracottatech.offheapstore.filesystem.Directory;
import com.terracottatech.offheapstore.filesystem.FileSystem;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.util.MemoryUnit;

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
