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
package org.terracotta.offheapstore.util;

import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;

public interface Generator {

  interface SpecialInteger {

    int value();
  }

  SpecialInteger generate(int i);

  StorageEngine<SpecialInteger, SpecialInteger> engine();

  Factory<StorageEngine<SpecialInteger, SpecialInteger>> factory();

  Generator GOOD_GENERATOR = new Generator() {

    @Override
    public String toString() {
      return "GoodGenerator";
    }

    @Override
    public SpecialInteger generate(final int i) {
      return new GoodInteger(i);
    }

    @Override
    public Factory<StorageEngine<SpecialInteger, SpecialInteger>> factory() {
      return this::engine;
    }

    @Override
    public StorageEngine<SpecialInteger, SpecialInteger> engine() {
      return new StorageEngine<SpecialInteger, SpecialInteger>() {

        @Override
        public void clear() {
          //no-op
        }

        @Override
        public Long writeMapping(SpecialInteger key, SpecialInteger value, int hash, int metadata) {
          return SplitStorageEngine.encoding(key.value(), value.value());
        }

        @Override
        public void attachedMapping(long encoding, int hash, int metadata) {
          //no-op
        }

        @Override
        public void freeMapping(long encoding, int hash, boolean removal) {
          //no-op
        }

        @Override
        public SpecialInteger readValue(long encoding) {
          return new GoodInteger(SplitStorageEngine.valueEncoding(encoding));
        }

        @Override
        public boolean equalsValue(Object value, long encoding) {
          return (value instanceof GoodInteger) && ((GoodInteger) value).value() == SplitStorageEngine.valueEncoding(encoding);
        }

        @Override
        public SpecialInteger readKey(long encoding, int hashCode) {
          return new GoodInteger(SplitStorageEngine.keyEncoding(encoding));
        }

        @Override
        public boolean equalsKey(Object key, long encoding) {
          return (key instanceof GoodInteger) && ((GoodInteger) key).value() == SplitStorageEngine.keyEncoding(encoding);
        }

        @Override
        public long getAllocatedMemory() {
          return 0;
        }

        @Override
        public long getOccupiedMemory() {
          return 0;
        }

        @Override
        public long getVitalMemory() {
          return 0;
        }

        @Override
        public long getDataSize() {
          return 0;
        }

        @Override
        public void invalidateCache() {
          //no-op
        }

        @Override
        public void bind(Owner owner) {
          //no-op
        }

        @Override
        public void destroy() {
          //no-op
        }

        @Override
        public boolean shrink() {
          return false;
        }
      };
    }
  };

  Generator BAD_GENERATOR = new Generator() {

    @Override
    public String toString() {
      return "BadGenerator";
    }

    @Override
    public SpecialInteger generate(final int i) {
      return new BadInteger(i);
    }

    @Override
    public Factory<StorageEngine<SpecialInteger, SpecialInteger>> factory() {
      return this::engine;
    }

    @Override
    public StorageEngine<SpecialInteger, SpecialInteger> engine() {
      return new StorageEngine<SpecialInteger, SpecialInteger>() {

        @Override
        public void clear() {
          //no-op
        }

        @Override
        public Long writeMapping(SpecialInteger key, SpecialInteger value, int hash, int metadata) {
          return SplitStorageEngine.encoding(key.value(), value.value());
        }

        @Override
        public void attachedMapping(long encoding, int hash, int metadata) {
          //no-op
        }

        @Override
        public void freeMapping(long encoding, int hash, boolean removal) {
          //no-op
        }

        @Override
        public SpecialInteger readValue(long encoding) {
          return new BadInteger(SplitStorageEngine.valueEncoding(encoding));
        }

        @Override
        public boolean equalsValue(Object value, long encoding) {
          return (value instanceof BadInteger) && ((BadInteger) value).value() == SplitStorageEngine.valueEncoding(encoding);
        }

        @Override
        public SpecialInteger readKey(long encoding, int hashCode) {
          return new BadInteger(SplitStorageEngine.keyEncoding(encoding));
        }

        @Override
        public boolean equalsKey(Object key, long encoding) {
          return (key instanceof BadInteger) && ((BadInteger) key).value() == SplitStorageEngine.keyEncoding(encoding);
        }

        @Override
        public long getAllocatedMemory() {
          return 0;
        }

        @Override
        public long getOccupiedMemory() {
          return 0;
        }

        @Override
        public long getVitalMemory() {
          return 0;
        }

        @Override
        public long getDataSize() {
          return 0;
        }

        @Override
        public void invalidateCache() {
          //no-op
        }

        @Override
        public void bind(Owner owner) {
          //no-op
        }

        @Override
        public void destroy() {
          //no-op
        }

        @Override
        public boolean shrink() {
          return false;
        }
      };
    }
  };

  Factory<StorageEngine<SpecialInteger, SpecialInteger>> BAD_FACTORY = BAD_GENERATOR::engine;

  class GoodInteger implements SpecialInteger {

    private final int n;

    GoodInteger(int i) {
      this.n = i;
    }

    @Override
    public int hashCode() {
      return n;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof GoodInteger && ((GoodInteger) o).n == n;
    }

    @Override
    public int value() {
      return n;
    }
  }

  class BadInteger implements SpecialInteger {
    public static final int HASHCODE = 42;

    private final int n;

    BadInteger(int i) {
      this.n = i;
    }

    @Override
    public int hashCode() {
      return HASHCODE;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof BadInteger && ((BadInteger) o).n == n;
    }

    @Override
    public int value() {
      return n;
    }
  }

}
