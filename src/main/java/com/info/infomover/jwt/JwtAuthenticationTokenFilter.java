package com.info.infomover.jwt;

import com.info.infomover.entity.User;
import com.info.infomover.util.UserUtil;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class JwtAuthenticationTokenFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        User loginUser = null;
        try {
            loginUser = TokenUtils.getLoginUser(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (loginUser != null) {
            UserUtil.setUser(loginUser);
        }

        filterChain.doFilter(request, response);
        UserUtil.clearAllUserInfo();
    }
}