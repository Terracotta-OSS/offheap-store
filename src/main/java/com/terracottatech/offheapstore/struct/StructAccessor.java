/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface StructAccessor<T> {

  boolean getBoolean(String name);
  
  void setBoolean(String name, boolean value);
  
  char getCharacter(String name);
  
  void setCharacter(String name, char value);
  
  int getInteger(String name);
  
  void setInteger(String name, int value);
  
  long getLong(String name);
  
  void setLong(String name, long value);

  double getDouble(String name);
  
  void setDouble(String name, double value);
  
  CharSequence getString(String name);
  
  void setString(String name, CharSequence value);
  
  ByteBuffer getBytes(String name);
  
  void setBytes(String name, ByteBuffer bytes);
}
