package com.weibo.rill.flow.service.util;

import com.weibo.rill.flow.common.constant.ReservedConstant;
import org.apache.commons.lang3.StringUtils;

public class DescriptorIdUtil {

    /**
     * change descriptorId to businessId
     */
    public static String changeDescriptorIdToBusinessId(String descriptorId) {
        return StringUtils.substringBefore(descriptorId, ReservedConstant.COLON);
    }

    private DescriptorIdUtil() { }
}
