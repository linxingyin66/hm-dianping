package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @ClassName LoginInterceptor
 * @Description
 * @Author LXY
 * @Date 2023/10/22 19:10
 **/
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //判断是否有用户登录
        if (UserHolder.getUser() == null) {
            //无用户，需要拦截  401-未登录
            response.setStatus(401);
            return false;
        }
        //有用户，放行
        return true;
    }

}
