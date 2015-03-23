/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

import org.terracotta.offheapstore.util.DebuggingUtils;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

import java.util.Random;

import org.junit.Test;

/**
 *
 * @author Chris Dennis
 */
public class DebuggingUtilsTest {

  @Test
  public void testPowerOfTwoBase2Conversions() {
    long size = 1L;
    for (String suffix : new String[] {"", "K", "M", "G", "T", "P", "E"}) {
      for (long value = 1; value < 1024; value <<= 1, size <<= 1) {
        if (size < 0) {
          break;
        }
        assertThat(DebuggingUtils.toBase2SuffixedString(size), is(value + suffix));
      }
    }
  }
  
  @Test
  public void testBase2Conversions() {
    long seed = System.nanoTime();
    System.out.println("testBase2Conversions: seed=" + seed);
    Random rndm = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      long n = Math.abs(rndm.nextLong());
      if (n < 0) {
        continue;
      }
      String formatted = DebuggingUtils.toBase2SuffixedString(n);
      switch (formatted.charAt(formatted.length() - 1)) {
        default:
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length())), 0));
          break;
        case 'K':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 10), Math.pow(2, 10) / 10));
          break;
        case 'M':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 20), Math.pow(2, 20) / 10));
          break;
        case 'G':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 30), Math.pow(2, 30) / 10));
          break;
        case 'T':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 40), Math.pow(2, 40) / 10));
          break;
        case 'P':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 50), Math.pow(2, 50) / 10));
          break;
        case 'E':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * Math.pow(2, 60), Math.pow(2, 60) / 10));
          break;
      }
    }
  }

  @Test
  public void testBase10Conversions() {
    long seed = System.nanoTime();
    System.out.println("testBase10Conversions: seed=" + seed);
    Random rndm = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      long n = rndm.nextLong();
      String formatted = DebuggingUtils.toBase10SuffixedString(n);
      switch (formatted.charAt(formatted.length() - 1)) {
        default :
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length())), 0));
          break;
        case 'k':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e3, 1e3));
          break;
        case 'M':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e6, 1e6));
          break;
        case 'G':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e9, 1e9));
          break;
        case 'T':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e12, 1e12));
          break;
        case 'P':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e15, 1e15));
          break;
        case 'E':
          assertThat((double) n, closeTo(Double.parseDouble(formatted.substring(0, formatted.length() - 1)) * 1e18, 1e18));
          break;
      }
    }
  }
}
