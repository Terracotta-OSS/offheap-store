/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * @author Chris Dennis
 */
public interface Persistent {

  void flush() throws IOException;

  void close() throws IOException;

  void persist(ObjectOutput output) throws IOException;

  void bootstrap(ObjectInput input) throws IOException;
}
