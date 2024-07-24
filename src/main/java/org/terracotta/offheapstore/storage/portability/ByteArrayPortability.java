/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
