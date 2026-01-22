/**
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tidesdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Handles loading of the native TidesDB JNI library.
 */
public class NativeLibrary {
    
    private static final String LIBRARY_NAME = "tidesdb_jni";
    private static volatile boolean loaded = false;
    private static final Object lock = new Object();
    
    /**
     * Loads the native library.
     * This method is idempotent and thread-safe.
     */
    public static void load() {
        if (loaded) {
            return;
        }
        
        synchronized (lock) {
            if (loaded) {
                return;
            }
            
            UnsatisfiedLinkError lastError = null;
            
            try {
                // First, try to load from java.library.path
                System.loadLibrary(LIBRARY_NAME);
                loaded = true;
                return;
            } catch (UnsatisfiedLinkError e) {
                lastError = e;
                // Fall through to try loading from resources
            }
            
            try {
                loadFromResources();
                loaded = true;
                return;
            } catch (IOException e) {
                // Fall through
            }
            
            // Throw error with details about what we tried
            String libraryPath = System.getProperty("java.library.path");
            throw new UnsatisfiedLinkError(
                "Failed to load TidesDB native library. " +
                "Make sure libtidesdb_jni is in java.library.path. " +
                "java.library.path: [" + libraryPath + "]. " +
                "Error: " + (lastError != null ? lastError.getMessage() : "unknown")
            );
        }
    }
    
    private static void loadFromResources() throws IOException {
        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        
        String libName;
        String libExtension;
        
        if (osName.contains("linux")) {
            libName = "lib" + LIBRARY_NAME;
            libExtension = ".so";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            libName = "lib" + LIBRARY_NAME;
            libExtension = ".dylib";
        } else if (osName.contains("windows")) {
            libName = LIBRARY_NAME;
            libExtension = ".dll";
        } else {
            throw new IOException("Unsupported operating system: " + osName);
        }
        
        String resourcePath = "/native/" + osName + "/" + osArch + "/" + libName + libExtension;
        
        try (InputStream is = NativeLibrary.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Native library not found in resources: " + resourcePath);
            }
            
            Path tempDir = Files.createTempDirectory("tidesdb-java");
            File tempFile = new File(tempDir.toFile(), libName + libExtension);
            tempFile.deleteOnExit();
            tempDir.toFile().deleteOnExit();
            
            Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.getAbsolutePath());
        }
    }
    
    /**
     * Returns whether the native library has been loaded.
     *
     * @return true if loaded
     */
    public static boolean isLoaded() {
        return loaded;
    }
}
