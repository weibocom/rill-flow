package com.weibo.rill.flow.common.model;

import lombok.Data;
//import javax.validation.constraints.NotNull;

import java.io.Serializable;

@Data
public class UserLoginRequest implements Serializable {

//    @NotNull(message = "账号不能为空")
    private String username;

//    @NotNull(message = "密码不能为空")
    private String password;

    private boolean autoLogin;

    private String type;

}