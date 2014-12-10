/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import java.nio.ByteBuffer;

/**
 * An object to ByteBuffer converter.
 *
 * @param <T> type handled by this converter
 *
 * @author Chris Dennis
 */
public interface Portability<T> {

  /**
   * Encodes an object of type {@code T} as a {@code ByteBuffer}.
   *
   * @param object object to be encoded
   * @return the encoded object
   */
  ByteBuffer encode(T object);

  /**
   * Decodes a {@code ByteBuffer} to an object of type {@code T}.
   *
   * @param buffer bytes to decode
   * @return the decoded object
   */
  T decode(ByteBuffer buffer);

  /**
   * Returns true if the encoded object once decoded would be
   * {@code Object.equals(Object)} to the supplied object.
   *
   * @param object object to compare to
   * @param buffer buffer containing encoded object
   * @return {@code true} if the two parameters are "equal"
   */
  boolean equals(Object object, ByteBuffer buffer);
}
