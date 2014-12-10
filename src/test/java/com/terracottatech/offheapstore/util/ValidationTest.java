/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.util;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class ValidationTest {
  
  @Test
  public void testValidatingInTests() {
    Assert.assertTrue(Validation.shouldValidate(Validation.class));
    Assert.assertTrue(Validation.shouldValidate(null));
  }
  
  @Test
  public void testValidations() {
    Validation.validate(true);
    try {
      Validation.validate(false);
      Assert.fail();
    } catch (AssertionError e) {
      //expected
    }
  }
}
