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
    return object ? TRUE.duplicate() : FALSE.duplicate();
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
