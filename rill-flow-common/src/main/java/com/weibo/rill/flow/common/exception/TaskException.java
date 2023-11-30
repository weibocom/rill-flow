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

package com.weibo.rill.flow.common.exception;

import com.weibo.rill.flow.common.model.BizError;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;


@ResponseStatus(HttpStatus.BAD_REQUEST)
public class TaskException extends RuntimeException {
    private final int errorCode;
    private final String executionId;

    public TaskException(final int errorCode, final String errorMsg) {
        super(errorMsg);
        this.errorCode = errorCode;
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(final int errorCode, final String errorMsg, final Throwable cause) {
        super(errorMsg, cause);
        this.errorCode = errorCode;
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(final int errorCode, final Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(final BizError bizError, final String errorMsg) {
        super(errorMsg);
        this.errorCode = bizError.getCode();
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(BizError bizError, String executionId, String errorMsg) {
        super(errorMsg);
        this.errorCode = bizError.getCode();
        this.executionId = executionId;
    }

    public TaskException(final BizError bizError) {
        super(bizError.getCauseMsg());
        this.errorCode = bizError.getCode();
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(final BizError bizError, final String errorMsg, final Throwable cause) {
        super(errorMsg, cause);
        this.errorCode = bizError.getCode();
        this.executionId = StringUtils.EMPTY;
    }

    public TaskException(final BizError bizError, final Throwable cause) {
        super(bizError.getCauseMsg(), cause);
        this.errorCode = bizError.getCode();
        this.executionId = StringUtils.EMPTY;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getExecutionId() {
        return executionId;
    }
}
