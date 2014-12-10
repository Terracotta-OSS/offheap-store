package com.terracottatech.offheapstore.storage.portability;

public interface WriteContext {

  void setLong(int offset, long value);
  
  void flush();
}
