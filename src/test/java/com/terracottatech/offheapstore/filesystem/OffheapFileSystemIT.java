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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OffheapFileSystemIT {

  @Test
  public void testCreateDirectory() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      for (int i = 0; i < 10; i++) {
        fs.getOrCreateDirectory("foo-" + Integer.toString(i));
      }
      for (int i = 0; i < 10; i++) {
        assertTrue(fs.directoryExists("foo-" + Integer.toString(i)));
      }
      fs.getOrCreateDirectory("foo");
      assertThat(fs.getOrCreateDirectory("foo"), notNullValue());
      try {
        fs.deleteDirectory("foo");
        assertFalse(fs.directoryExists("foo"));
      } catch (FileNotFoundException e) {
        throw new AssertionError(e);
      }
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testGetAllDirectoryNames() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      for (int i = 0; i < 10; i++) {
        fs.getOrCreateDirectory("foo-" + Integer.toString(i));
      }
      List<String> dirNames;
      Set<String> names = fs.listDirectories();
      dirNames = new ArrayList<String>(names);
      Collections.sort(dirNames);
      assertThat(dirNames, hasSize(10));
      for (int i = 0; i < 10; i++) {
        assertThat(dirNames.get(i), is("foo-" + Integer.toString(i)));
      }
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testDeleteDirectory() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      List<String> dirNames;
      Set<String> names = fs.listDirectories();
      dirNames = new ArrayList<String>(names);
      for (int i = 0; i < dirNames.size(); i++) {
        String dirName = dirNames.get(i);
        fs.deleteDirectory(dirName);
        assertFalse(fs.directoryExists(dirName));
      }
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testDelete() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      for (int i = 0; i < 10; i++) {
        fs.getOrCreateDirectory("bar-" + Integer.toString(i));
      }
      fs.delete();
      assertThat(fs.listDirectories(), empty());
    } finally {
      fs.delete();
    }
  }
}
