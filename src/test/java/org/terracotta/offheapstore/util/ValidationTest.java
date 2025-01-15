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

import org.terracotta.offheapstore.util.Validation;
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
