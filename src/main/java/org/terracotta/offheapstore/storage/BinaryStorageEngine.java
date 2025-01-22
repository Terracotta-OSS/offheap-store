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
package org.terracotta.offheapstore.storage;

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
