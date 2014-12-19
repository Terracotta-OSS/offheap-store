package com.terracottatech.offheapstore.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * <p>
 * This interface provides top-level access to the OffHeap file system. It contains methods to create/delete
 * directories, check for their existence etc.
 * <p>
 * It is generally true for the methods in this interface that an <code>IOException</code> will be thrown if the
 * processing can not complete due to an I/O error caused due to events beyond programmers control.
 * 
 * @author dkumar
 */
public interface FileSystem {

  /**
   * Creates a new, empty directory or returns a reference to the named directory if it already exists. Nested
   * directories are not allowed, hence a directory name of "foo/bar" will result in the creation of a directory named
   * "foo/bar".
   * 
   * @param name name of the directory to be created.
   * @return a reference to the new or existing directory
   * @throws IOException if an I/O error occurs.
   */
  Directory getOrCreateDirectory(String name) throws IOException;

  /**
   * Returns a listing of all directory names present in the system.
   * 
   * @return a list containing names of the directories.
   * @throws IOException if an I/O error occurs.
   */
  Set<String> listDirectories() throws IOException;

  /**
   * Deletes a particular directory.
   * 
   * @param name of the directory to be deleted
   * @throws IOException if an I/O error occurs.
   * @throws FileNotFoundException if the directory does not exist.
   */
  void deleteDirectory(String name) throws IOException, FileNotFoundException;

  /**
   * Deletes the off heap file system with its directories and files and frees up the memory. If a directory creation
   * operation is interleaved with this method, then the new directory may or may not be deleted.
   * 
   * @throws IOException if an I/O error occurs.
   */
  void delete() throws IOException;

  /**
   * Checks if a directory exists in the file system
   * 
   * @param name of the directory to check for existence
   * @return true if the directory exists, false otherwise.
   * @throws IOException if an I/O error occurs.
   */
  boolean directoryExists(String name) throws IOException;

}
