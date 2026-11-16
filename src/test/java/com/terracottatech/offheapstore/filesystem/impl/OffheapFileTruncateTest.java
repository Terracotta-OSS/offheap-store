/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2026
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

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import com.terracottatech.offheapstore.filesystem.Directory;
import com.terracottatech.offheapstore.filesystem.File;
import com.terracottatech.offheapstore.filesystem.FileSystem;
import com.terracottatech.offheapstore.filesystem.SeekableInputStream;
import com.terracottatech.offheapstore.filesystem.SeekableOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Tests truncation and end-of-file behavior for {@link OffheapFile} streams. */
public class OffheapFileTruncateTest {

  @Test
  public void testTruncateToZeroReleasesAllBlocks() throws IOException {
    PageSource pageSource = new UpfrontAllocatingPageSource(new HeapBufferSource(), 10 * 1024 * 1024, 1024 * 1024);
    FileSystem fs = new OffheapFileSystem(pageSource, 8192, 256 * 1024, 1);
    Directory dir = fs.getOrCreateDirectory("testDir");
    File file = dir.getOrCreateFile("testFile");

    // Write 16KB of data across 2 blocks (block 0: 0-8191, block 1: 8192-16383)
    try (SeekableOutputStream out = file.getOutputStream()) {
      byte[] data = new byte[16384];
      out.write(data);
      out.flush();
    }

    assertThat(file.length(), is(16384L));
    assertThat(((OffheapFile) file).numBlocks(), is(2));

    // Reset/truncate file length to 0 bytes
    try (SeekableOutputStream out = file.getOutputStream()) {
      out.reset();
      out.flush();
    }

    assertThat(file.length(), is(0L));
    // Verify block 0 is freed: blockMap should now have 0 blocks remaining
    assertThat("Truncating to 0 bytes must release all blocks including block 0",
        ((OffheapFile) file).numBlocks(), is(0));
  }

  @Test
  public void testBulkReadPastEndThrowsEof() throws Exception {
    PageSource pageSource = new UpfrontAllocatingPageSource(new HeapBufferSource(), 10 * 1024 * 1024, 1024 * 1024);
    FileSystem fs = new OffheapFileSystem(pageSource, 8192, 256 * 1024, 1);
    Directory dir = fs.getOrCreateDirectory("testDir");
    File file = dir.getOrCreateFile("testFile");

    try (SeekableOutputStream out = file.getOutputStream()) {
      out.write(42);
    }

    SeekableInputStream input = file.getInputStream();
    FutureTask<IOException> read = new FutureTask<>(() -> {
      try {
        input.readBytes(new byte[2], 0, 2);
        return null;
      } catch (IOException e) {
        return e;
      } finally {
        input.close();
      }
    });
    Thread reader = new Thread(read, "offheap-input-eof-test");
    reader.setDaemon(true);
    reader.start();

    IOException failure;
    try {
      failure = read.get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw new AssertionError(e.getCause());
    }
    assertThat(failure instanceof EOFException, is(true));
  }
}
