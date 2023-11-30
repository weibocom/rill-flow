/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.olympicene.storage.exception;

import com.weibo.rill.flow.interfaces.model.exception.DAGException;

public class StorageException extends DAGException {

    public StorageException(int errorCode, String message) {
        super(errorCode, message);
    }

    public StorageException(int errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public StorageException(int errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
