package com.terracottatech.offheapstore.util;

import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine.Owner;

public interface Generator {
  public interface SpecialInteger {
    int value();
  }
  
  SpecialInteger generate(int i);

  public static final Generator GOOD_GENERATOR = new Generator() {
    @Override
    public SpecialInteger generate(final int i) {
      return new GoodInteger(i);
    }
  };
  
  public static final StorageEngine<SpecialInteger, SpecialInteger> GOOD_ENGINE = new StorageEngine<SpecialInteger, SpecialInteger>() {

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
  
  public static final Factory<StorageEngine<SpecialInteger, SpecialInteger>> GOOD_FACTORY = new Factory<StorageEngine<SpecialInteger, SpecialInteger>>() {
    @Override
    public StorageEngine<SpecialInteger, SpecialInteger> newInstance() {
      return GOOD_ENGINE;
    }
  };
  
  public static final Generator BAD_GENERATOR = new Generator() {
    @Override
    public SpecialInteger generate(final int i) {
      return new BadInteger(i);
    }
  };

  public static final StorageEngine<SpecialInteger, SpecialInteger> BAD_ENGINE = new StorageEngine<SpecialInteger, SpecialInteger>() {

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
  
  public static final Factory<StorageEngine<SpecialInteger, SpecialInteger>> BAD_FACTORY = new Factory<StorageEngine<SpecialInteger, SpecialInteger>>() {
    @Override
    public StorageEngine<SpecialInteger, SpecialInteger> newInstance() {
      return BAD_ENGINE;
    }
  };
    
  static class GoodInteger implements SpecialInteger {
    
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
      if (o instanceof GoodInteger) {
        return ((GoodInteger) o).n == n;
      } else {
        return false;
      }
    }

    @Override
    public int value() {
      return n;
    }
  }
  
  static class BadInteger implements SpecialInteger {
    
    private final int n;
    
    BadInteger(int i) {
      this.n = i;
    }

    @Override
    public int hashCode() {
      return 42;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof BadInteger) {
        return ((BadInteger) o).n == n;
      } else {
        return false;
      }
    }

    @Override
    public int value() {
      return n;
    }
  }
  
}
