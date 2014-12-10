/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import java.nio.ByteBuffer;

/**
 * A simple {@code byte[]} portability.
 *
 * @author Chris Dennis
 */
public class ByteArrayPortability implements Portability<byte[]> {

  public static final ByteArrayPortability INSTANCE = new ByteArrayPortability();
  
  protected ByteArrayPortability() {
    //singleton
  }
  
  @Override
  public ByteBuffer encode(byte[] object) {
    return ByteBuffer.wrap(object);
  }

  @Override
  public byte[] decode(ByteBuffer buffer) {
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);
    return data;
  }

  /**
   * Byte arrays do not have a content-based equals method so this throws
   * {@code UnsupportedOperationException}.
   * <p>
   * The lack of any implementation only prevents this portability being used as
   * a key portability.  Using byte arrays as keys in map would be extremely
   * questionable in any case.  If necessary this portability could be
   * sub-classed to do deep array comparison.
   */
  @Override
  public boolean equals(Object value, ByteBuffer readBuffer) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Byte arrays cannot be compared in their serialized forms - byte array eqaulity is identity based.");
  }
}
