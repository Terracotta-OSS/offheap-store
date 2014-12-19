package com.terracottatech.offheapstore.filesystem;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OffheapOutputStreamIT {

  private static final byte[] DATA_ARRAY;
  static {
    try{
      DATA_ARRAY = "test-data".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }
  
  @Test
  public void testWriteByte() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testWriteByte").getOrCreateFile("testWriteByte");
      SeekableOutputStream out = file.getOutputStream(); 
      out.write(100);
      out.flush();
      assertThat(file.length(), is(1L));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testWriteBytes() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testWriteBytes").getOrCreateFile("testWriteBytes");
      SeekableOutputStream out = file.getOutputStream(); 
      out.write(DATA_ARRAY);
      out.flush();
      assertThat(file.length(), is((long) DATA_ARRAY.length));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testWriteBytesAndClearFile() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testWriteBytesAndClearFile").getOrCreateFile("testWriteBytesAndClearFile");
      SeekableOutputStream out = file.getOutputStream(); 
      out.reset();
      out.write(100);
      out.flush();
      assertThat(file.length(), is(1L));
      file.truncate();
      assertThat(file.length(), is(0L));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testReset() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testReset").getOrCreateFile("testReset");
      SeekableOutputStream out = file.getOutputStream();
      out.write(DATA_ARRAY);
      out.flush();
      assertThat(file.length(), is((long) DATA_ARRAY.length));
      out.reset();
      assertThat(file.length(), is(0L));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testSeek() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testSeek").getOrCreateFile("testSeek");
      SeekableOutputStream out = file.getOutputStream();
      out.write(DATA_ARRAY);
      // validate the contract that negative seeks raise IOExcpetion
      try {
        out.seek(-1);
        fail("testSeek() should have thrown an IOException");
      } catch (IOException e) {
        // detected that it generated IOExcpetion, continue...
      }

      // validate the contract that offset can be set beyond the end of the file
      out.reset();
      out.write(DATA_ARRAY);
      out.flush();
      assertThat(file.length(), is((long) DATA_ARRAY.length));
      // seek beyond the end of this file
      out.seek(2 * DATA_ARRAY.length);
      // seeks alone do not change a file's length
      assertThat(file.length(), is((long) DATA_ARRAY.length));
      // ...but writes do
      out.write((byte) 100);
      out.flush();
      // validate that file length has changed
      assertThat(file.length(), is((long) 2 * DATA_ARRAY.length + 1));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testWrite() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testWrite").getOrCreateFile("testWrite");
      SeekableOutputStream out = file.getOutputStream();
      out.write(DATA_ARRAY);
      out.flush();
      assertThat(file.length(), is((long) DATA_ARRAY.length));
    } finally {
      fs.delete();
    }
  }
}
