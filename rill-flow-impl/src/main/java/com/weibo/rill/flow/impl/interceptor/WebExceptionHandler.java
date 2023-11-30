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

package com.weibo.rill.flow.impl.interceptor;

import com.google.common.collect.ImmutableSet;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import com.weibo.rill.flow.interfaces.model.exception.DAGException;
import com.weibo.rill.flow.common.exception.TaskException;
import com.weibo.rill.flow.common.model.BizError;
import com.weibo.rill.flow.common.model.HttpResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;

import javax.servlet.ServletException;
import java.util.Set;

@Slf4j
@ControllerAdvice
@ResponseBody
public class WebExceptionHandler {
    private static final Set<Integer> PASS_THROUGH_ERROR_CODE = ImmutableSet.of(
            BizError.ERROR_DEGRADED.getCode(), BizError.ERROR_DATA_FORMAT.getCode(), BizError.ERROR_DATA_RESTRICTION.getCode());

    /**
     * 框架异常处理
     */
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    @ExceptionHandler({
            TypeMismatchException.class,
            ServletRequestBindingException.class,
            HttpMessageConversionException.class,
            HttpRequestMethodNotSupportedException.class,
            HttpMediaTypeException.class,
            ServletException.class,
    })
    public HttpResponse handleRequestException(final Exception ex) {
        if (ex instanceof TypeMismatchException) {
            log.warn("bad framework request, TypeMismatchException.", ex);
            return HttpResponse.error(BizError.ERROR_DATA_FORMAT.getCode(), ex.getMessage());
        } else if (ex instanceof MissingServletRequestParameterException) { // 接口参数未传报该异常 不用打印整个异常栈 打印异常信息即可
            log.warn("bad framework request, MissingServletRequestParameterException. errorMsg:{}", ex.getMessage());
            return HttpResponse.error(BizError.ERROR_DATA_FORMAT.getCode(), ex.getMessage());
        } else if (ex instanceof HttpMessageNotReadableException) {
            log.warn("bad framework request, HttpMessageNotReadableException. errorMsg:{}", ex.getMessage());
            return HttpResponse.error(BizError.ERROR_DATA_FORMAT.getCode(), ex.getMessage());
        } else if (ex instanceof ServletRequestBindingException) {
            log.warn("bad framework request, ServletRequestBindingException.", ex);
            return HttpResponse.error(BizError.ERROR_DATA_FORMAT.getCode(), ex.getMessage());
        } else if (ex instanceof HttpRequestMethodNotSupportedException) {
            log.warn("bad framework request, HttpRequestMethodNotSupportedException.", ex);
            return HttpResponse.error(BizError.ERROR_UNSUPPORTED.getCode(), ex.getMessage());
        } else if (ex instanceof HttpMediaTypeException) {
            log.warn("bad framework request, HttpMediaTypeException.", ex);
            return HttpResponse.error(BizError.ERROR_UNSUPPORTED.getCode(), ex.getMessage());
        } else {
            log.warn("bad framework request", ex);
        }

        return HttpResponse.error(BizError.ERROR_INTERNAL);
    }

    /**
     * 业务异常处理
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({
            TaskException.class,
    })
    public HttpResponse handleBizException(final TaskException ex) {
        int errorCode = ex.getErrorCode();
        log.error("publish error message={}, executionId={}", ex.getMessage(), ex.getExecutionId());
        return PASS_THROUGH_ERROR_CODE.contains(errorCode)
                ? HttpResponse.error(errorCode, ex.getMessage(), true)
                : HttpResponse.error(errorCode, ex.getMessage());
    }

    /**
     * HystrixRuntimeException异常处理
     *
     * @param ex
     * @param webRequest
     * @return
     */
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({
            HystrixRuntimeException.class,
    })
    public HttpResponse handleHystrixRuntimeException(final HystrixRuntimeException ex, WebRequest webRequest) {
        log.warn(
                "HystrixRuntimeException message={}, failureType={}, fallbackException={}, commandClass={}, requestInfo={}, requestParam={}",
                ex.getMessage(), ex.getFailureType().name(),
                ex.getFallbackException() == null ? "" : ex.getFallbackException().getCause().getMessage(),
                ex.getImplementingClass().getName(), webRequest.getDescription(true), webRequest.getParameterMap());

        // fallback 抛出异常的情况
        return HttpResponse.error(BizError.ERROR_HYSTRIX);
    }


    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({
            DAGException.class,
    })
    public HttpResponse handleDAGException(final DAGException ex, WebRequest webRequest) {
        log.debug("DAGException message={}, cause={} ", ex.getMessage(), ex.getCause(), ex);
        return HttpResponse.error(ex.getErrorCode(), ex.getMessage());
    }

    /**
     * 参数检查错误
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({BindException.class})
    public HttpResponse handleSpringValidCheckException(final Throwable ex) {
        log.error("spring mvc request exception.", ex);
        return HttpResponse.error(BizError.ERROR_MISSING_PARAMETER);
    }

    /**
     * 非预期异常处理
     */
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({
            Exception.class,
            Throwable.class,
    })
    public HttpResponse handleGeneralException(final Throwable ex) {
        log.error("internal server error", ex);
        return HttpResponse.error(BizError.ERROR_INTERNAL);
    }

}
