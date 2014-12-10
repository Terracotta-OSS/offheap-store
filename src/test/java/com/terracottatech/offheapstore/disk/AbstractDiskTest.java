/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractDiskTest {

  protected File dataFile;

  @Before
  public void createDataFile() throws IOException {
    dataFile = File.createTempFile(getClass().getSimpleName(), ".data");
    dataFile.deleteOnExit();
  }

  @After
  public void destroyDataFile() {
    dataFile.delete();
  }
}
