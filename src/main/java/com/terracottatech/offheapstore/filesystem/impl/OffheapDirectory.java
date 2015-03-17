package com.terracottatech.offheapstore.filesystem.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.offheapstore.filesystem.Directory;
import com.terracottatech.offheapstore.filesystem.File;
import org.terracotta.offheapstore.paging.PageSource;

public class OffheapDirectory implements Directory {
  private final String                   name;
  private final PageSource               pageSource;
  private final Map<String, OffheapFile> files = new HashMap<String, OffheapFile>();
  private final int                      blockSize;
  private final int                      maxDataPageSize;
  private final int                      concurrency;
  private static final Logger LOGGER      = LoggerFactory.getLogger(OffheapDirectory.class);

  OffheapDirectory(String name, PageSource source, int blockSize, int maxDataPageSize, int concurrency) {
    this.name = name;
    this.pageSource = source;
    this.blockSize = blockSize;
    this.maxDataPageSize = maxDataPageSize;
    this.concurrency = concurrency;
    LOGGER.info("Creating OffheapDirectory: " + name + "\n\n");
  }

  @Override
  public synchronized File getOrCreateFile(String name) throws IOException {
    OffheapFile existing = files.get(name);
    if (existing == null) {
      existing = new OffheapFile(this, name, pageSource, blockSize, maxDataPageSize, concurrency);
      OffheapFile racer = files.put(name, existing);
      if (racer != null) {
        existing.delete();
        existing = racer;
      }
    }
    return existing;
  }

  @Override
  public synchronized void deleteAllFiles() throws IOException {
    for (Iterator<OffheapFile> it = files.values().iterator(); it.hasNext();) {
      OffheapFile removed = it.next();
      it.remove();
      removed.delete();
    }
  }

  @Override
  public void deleteFile(String name) throws IOException, FileNotFoundException {
    OffheapFile file = files.remove(name);
    if (file == null) throw new FileNotFoundException(name);
    file.delete();
  }

  @Override
  public boolean fileExists(String name) {
    return files.containsKey(name);
  }

  @Override
  public Set<String> listFiles() throws IOException {
    return Collections.unmodifiableSet(files.keySet());
  }

  @Override
  public synchronized long getSizeInBytes() {
    long size = 0;
    Iterator<OffheapFile> it = files.values().iterator();
    while (it.hasNext()) {
      size += it.next().getSizeInBytes();
    }
    return size;
  }

  public String getName() {
    return name;
  }

}
