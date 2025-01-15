/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package org.terracotta.offheapstore.storage.portability;

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
