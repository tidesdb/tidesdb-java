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
 * Callback interface for commit hooks (Change Data Capture).
 * Invoked synchronously after a transaction commits to a column family.
 * The hook receives the full batch of committed operations atomically.
 */
@FunctionalInterface
public interface CommitHook {
    
    /**
     * Called after a transaction commits to a column family.
     *
     * @param ops array of committed operations
     * @param commitSeq monotonic commit sequence number
     * @return 0 on success, non-zero on failure (logged as warning, does not roll back)
     */
    int onCommit(CommitOp[] ops, long commitSeq);
}
