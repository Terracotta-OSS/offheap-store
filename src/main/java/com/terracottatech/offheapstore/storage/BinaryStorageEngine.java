/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface BinaryStorageEngine {
  
  int readKeyHash(long encoding);

  ByteBuffer readBinaryKey(long encoding);
  
  ByteBuffer readBinaryValue(long encoding);
  
  boolean equalsBinaryKey(ByteBuffer probeKey, long encoding);

  Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata);
  
  Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata);
}
