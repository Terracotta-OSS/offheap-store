package com.terracottatech.offheapstore.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * This interface facilitates creation of random access files in the OffHeap file system. It provides methods for
 * querying the properties of the files, checking for their existence, deleting them, getting their length etc.
 * 
 * @author dkumar
 */
public interface Directory {

  /**
   * Creates a new, empty file or returns a reference to the named file if it already exists.
   * 
   * @param name name of the File to be created
   * @return reference to the new or existing file
   * @throws IOException if an error occurred while processing the request.
   */
  File getOrCreateFile(String name) throws IOException;

  /**
   * Deletes a File. This method will block until all the streams associated with the file have been closed. 
   * 
   * @param name of the file to be deleted.
   * @throws IOException if an I/O error occurs.
   * @throws FileNotFoundException if the file is not found.
   */
  void deleteFile(String name) throws IOException, FileNotFoundException;

  /**
   * Checks if a File exists in the directory
   * 
   * @param name name of the file to be checked for existence.
   * @return true if the file exists in the directory, false otherwise.
   */
  boolean fileExists(String name);

  /**
   * Retrieves the names of files present in the directory
   * 
   * @return set containing names of the files.
   * @throws IOException if an error occurs while processing the request.
   */
  Set<String> listFiles() throws IOException;

  /**
   * Reports the total size of the directory
   * 
   * @return sum of sizes of files in bytes.
   */
  long getSizeInBytes();

  /**
   * Deletes all files present under this directory
   * 
   * @throws IOException if an I/O error occurs
   */
  public void deleteAllFiles() throws IOException;

}
