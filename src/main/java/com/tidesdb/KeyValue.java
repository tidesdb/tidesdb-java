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

/**
 * A key-value pair returned by the combined iterator retrieval method.
 */
public class KeyValue {

    private final byte[] key;
    private final byte[] value;

    public KeyValue(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Gets the key bytes.
     *
     * @return the key
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Gets the value bytes.
     *
     * @return the value
     */
    public byte[] getValue() {
        return value;
    }
}
