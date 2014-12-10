/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.paging.OffHeapStorageArea;

/**
 *  
 * @author cdennis
 */
public abstract class Field {
  private final int length;
  private final int offset;
  
  Field(int length, int offset) {
    this.offset = offset;
    this.length = length;
  }

  public long address(long base) {
    return base + offset();
  }

  public int length() {
    return length;
  }

  public int offset() {
    return offset;
  }

  public void initialize(OffHeapStorageArea storage, long base) {
    //no-op
  }

  public void free(OffHeapStorageArea storage, long address) {
    //no-op
  }
  
  public static class BooleanField extends Field {

    BooleanField(int offset) {
      super(1, offset);
    }

    public boolean read(OffHeapStorageArea storage, long base) {
      return readBoolean(storage, address(base));
    }
    
    public void write(OffHeapStorageArea storage, long base, boolean value) {
      writeBoolean(storage, address(base), value);
    }
  }

    public static class CharacterField extends Field {

    CharacterField(int offset) {
      super(2, offset);
    }

    public char read(OffHeapStorageArea storage, long base) {
      return readCharacter(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, char value) {
      writeCharacter(storage, address(base), value);
    }
  }

  public static class IntegerField extends Field {

    IntegerField(int offset) {
      super(4, offset);
    }

    public int read(OffHeapStorageArea storage, long base) {
      return readInteger(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, int value) {
      writeInteger(storage, address(base), value);
    }
  }

  public static class LongField extends Field {

    LongField(int offset) {
      super(8, offset);
    }

    public long read(OffHeapStorageArea storage, long base) {
      return readLong(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, long value) {
      writeLong(storage, address(base), value);
    }
  }
  
  public static class DoubleField extends Field {

    DoubleField(int offset) {
      super(8, offset);
    }

    public double read(OffHeapStorageArea storage, long base) {
      return readDouble(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, double value) {
      writeDouble(storage, address(base), value);
    }
  }
  
  public static class StringField extends Field {
    
    StringField(int offset) {
      super(8, offset);
    }

    public CharSequence read(OffHeapStorageArea storage, long base) {
      return readString(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, CharSequence value) {
      writeString(storage, address(base), value);
    }
    
    @Override
    public void initialize(OffHeapStorageArea storage, long base) {
      writeLong(storage, address(base), -1L);
    }

    @Override
    public void free(OffHeapStorageArea storage, long address) {
      write(storage, address, null);
    }
  }
  
  public static class BytesField extends Field {
    
    BytesField(int offset) {
      super(8, offset);
    }

    public ByteBuffer read(OffHeapStorageArea storage, long base) {
      return readBytes(storage, address(base));
    }

    public void write(OffHeapStorageArea storage, long base, ByteBuffer value) {
      writeBytes(storage, address(base), value);
    }

    @Override
    public void initialize(OffHeapStorageArea storage, long base) {
      writeLong(storage, address(base), -1L);
    }

    @Override
    public void free(OffHeapStorageArea storage, long address) {
      write(storage, address, null);
    }
  }
  
  public static boolean readBoolean(OffHeapStorageArea storage, long address) {
    return storage.readByte(address) != 0;
  }
  
  public static void writeBoolean(OffHeapStorageArea storage, long address, boolean value) {
    storage.writeByte(address, (byte) (value ? 0xFF : 0x00));
  }
  
  public static char readCharacter(OffHeapStorageArea storage, long address) {
    return (char) storage.readShort(address);
  }
  
  public static void writeCharacter(OffHeapStorageArea storage, long address, char value) {
    storage.writeShort(address, (short) value);
  }
  
  public static int readInteger(OffHeapStorageArea storage, long address) {
    return storage.readInt(address);
  }
  
  public static void writeInteger(OffHeapStorageArea storage, long address, int value) {
    storage.writeInt(address, value);
  }

  public static long readLong(OffHeapStorageArea storage, long address) {
    return storage.readLong(address);
  }

  public static void writeLong(OffHeapStorageArea storage, long address, long value) {
    storage.writeLong(address, value);
  }

  public static double readDouble(OffHeapStorageArea storage, long address) {
    return Double.longBitsToDouble(storage.readLong(address));
  }

  public static void writeDouble(OffHeapStorageArea storage, long address, double value) {
    storage.writeLong(address, Double.doubleToRawLongBits(value));
  }

  public static CharSequence readString(OffHeapStorageArea storage, long address) {
    ByteBuffer bytes = readBytes(storage, address);
    if (bytes == null) {
      return null;
    } else {
      return bytes.asCharBuffer();
    }
  }

  public static void writeString(OffHeapStorageArea storage, long address, CharSequence value) {
    if (value == null) {
      writeBytes(storage, address, null);
    } else {
      ByteBuffer buffer = ByteBuffer.allocate(value.length() * 2);
      buffer.asCharBuffer().append(value);
      writeBytes(storage, address, buffer);
    }
  }

  public static ByteBuffer readBytes(OffHeapStorageArea storage, long address) {
    long bytesAddress = storage.readLong(address);
    if (bytesAddress == -1L) {
      return null;
    } else {
      int length = storage.readInt(bytesAddress);
      return storage.readBuffer(bytesAddress + 4, length);
    }
  }

  public static void writeBytes(OffHeapStorageArea storage, long address, ByteBuffer value) {
    long newValue;
    if (value == null) {
      newValue = -1L;
    } else {
      int length = value.remaining();
      newValue = storage.allocate(length + (Integer.SIZE / Byte.SIZE));
      if (newValue >= 0) {
        storage.writeInt(newValue, length);
        storage.writeBuffer(newValue + (Integer.SIZE / Byte.SIZE), value);
      } else {
        //XXX pick better exception here
        throw new IllegalStateException();
      }
    }
    long oldValue = storage.readLong(address);
    storage.writeLong(address, newValue);
    if (oldValue >= 0) {
      storage.free(oldValue);
    }
  }
}
