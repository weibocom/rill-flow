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

package com.weibo.rill.flow.olympicene.ddl.exception;

import com.weibo.rill.flow.interfaces.model.exception.DAGException;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;

public class DDLException extends DAGException {

    public DDLException(int errorCode, String errorMsg) {
        super(errorCode, errorMsg);
    }

    public DDLException(DDLErrorCode DDLErrorCode) {
        super(DDLErrorCode.getCode(), DDLErrorCode.getMessage());
    }

    public DDLException(DDLErrorCode DDLErrorCode, Throwable cause) {
        super(DDLErrorCode.getCode(), DDLErrorCode.getMessage(), cause);
    }

    public DDLException(int errorCode, String errorMsg, Throwable cause) {
        super(errorCode, errorMsg, cause);
    }

    public DDLException(int errorCode, Throwable cause) {
        super(errorCode, cause);
    }
}
