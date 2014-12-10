package com.terracottatech.offheapstore;

public final class Metadata {

  public static final int PINNED = 1 << (Integer.SIZE - 2);

  private Metadata() {
    //static class
  }
}
