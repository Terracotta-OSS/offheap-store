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

import java.io.IOException;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import com.terracottatech.offheapstore.filesystem.Directory;
import com.terracottatech.offheapstore.filesystem.File;
import com.terracottatech.offheapstore.filesystem.FileSystem;
import com.terracottatech.offheapstore.filesystem.SeekableOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests verifying issue fixes in {@link OffheapFile}.
 *
 * <h3>Issue Description</h3>
 * Truncating an {@link OffheapFile} to zero bytes (e.g. via {@code seek(0)}, {@code reset()},
 * or {@code setLength(0)}) fails to deallocate block 0 from the off-heap block map, causing
 * permanent memory leaks in off-heap file storage.
 *
 * <h3>Root Cause</h3>
 * In {@link OffheapFile#setLength(long)}:
 * <pre>{@code
 *   int newBlocks = (int) ((newLength + blockSize - 1) / blockSize);
 *   int oldBlocks = blockMap.size();
 *   if (newBlocks < oldBlocks) {
 *     for (int i = oldBlocks - 1; i > newBlocks; i--) { // Off-by-one: 'i > newBlocks' instead of 'i >= newBlocks'!
 *       FileBlock block = blockMap.remove(i);
 *       if (block != null) {
 *         block.free();
 *       }
 *     }
 *   }
 * }</pre>
 * When truncating to length 0, {@code newBlocks == 0}. Because the loop condition is {@code i > 0},
 * iteration stops at {@code i == 1}. Block index 0 is never removed from {@code blockMap} and never
 * freed via {@code block.free()}. Over time, repeated file overwrites/truncations leak off-heap pages.
 *
 * <h3>Expected Behavior</h3>
 * When {@code setLength(newLength)} reduces the file size to {@code newBlocks}, all blocks with indices
 * {@code i >= newBlocks} must be removed and freed. For {@code newLength == 0}, {@code blockMap.size()}
 * must become 0.
 */
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
}
