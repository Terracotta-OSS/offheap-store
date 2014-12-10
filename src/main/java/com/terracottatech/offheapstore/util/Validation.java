/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

/**
 *
 * @author Chris Dennis
 */
public final class Validation {
  
  private static final boolean VALIDATE_ALL;
  static {
    boolean validate;
    try {
      Class.forName("com.terracottatech.offheapstore.util.ValidationTest");
      validate = true;
    } catch (Throwable t) {
      validate = false;
    }
    VALIDATE_ALL = validate;
  }
  
  public static boolean shouldValidate(Class<?> klazz) {
    return VALIDATE_ALL || Boolean.getBoolean(klazz.getName() + ".VALIDATE");
  }
  
  public static void validate(boolean assertion) {
    if (!assertion) {
      throw new AssertionError();
    }
  }
}
