package com.terracottatech.offheapstore.storage.portability;

import java.nio.ByteBuffer;

public class BooleanPortability implements Portability<Boolean> {

  public static final BooleanPortability INSTANCE = new BooleanPortability();

  private static final ByteBuffer TRUE = ByteBuffer.allocateDirect(1);
  private static final ByteBuffer FALSE = ByteBuffer.allocateDirect(1);
  static {
    TRUE.put((byte) 1).flip();
    FALSE.put((byte) 0).flip();
  }
  
  private BooleanPortability() {
    //singleton
  }
  
  @Override
  public ByteBuffer encode(Boolean object) {
    return object.booleanValue() ? TRUE.duplicate() : FALSE.duplicate();
  }

  @Override
  public Boolean decode(ByteBuffer buffer) {
    if (buffer.get(0) > 0) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  @Override
  public boolean equals(Object object, ByteBuffer buffer) {
    return object.equals(decode(buffer));
  }
}
