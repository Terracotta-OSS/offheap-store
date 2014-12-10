package com.terracottatech.offheapstore.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;

public class OffheapFileSystemIT {

  @Test
  public void testCreateDirectory() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      for (int i = 0; i < 10; i++) {
        fs.getOrCreateDirectory("foo-" + Integer.toString(i));
      }
      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(fs.directoryExists("foo-" + Integer.toString(i)));
      }
      fs.getOrCreateDirectory("foo");
      Assert.assertNotNull(fs.getOrCreateDirectory("foo"));
      try {
        fs.deleteDirectory("foo");
        Assert.assertFalse(fs.directoryExists("foo"));
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
      Assert.assertEquals(10, dirNames.size());
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("foo-" + Integer.toString(i), dirNames.get(i));
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
        Assert.assertFalse(fs.directoryExists(dirName));
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
      Assert.assertEquals(0, fs.listDirectories().size());
    } finally {
      fs.delete();
    }
  }
}
