package com.info.infomover.security.authentication.refresh;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.TokenAuthenticationProcessingFilter;

public class TokenRefreshAuthenticationProcessingFilter extends TokenAuthenticationProcessingFilter {

	private final AuthenticationSuccessHandler successHandler;

	public TokenRefreshAuthenticationProcessingFilter(String defaultProcessUrl,
			final AuthenticationSuccessHandler successHandler, final AuthenticationFailureHandler failureHandler,
			final JwtTokenGenerater jwtTokenGenerater) {
		super(new AntPathRequestMatcher(defaultProcessUrl), failureHandler, jwtTokenGenerater);
		this.successHandler = successHandler;
	}

	@Override
	protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, FilterChain chain,
			Authentication authResult) throws IOException, ServletException {
		successHandler.onAuthenticationSuccess(request, response, authResult);
		SecurityContextHolder.clearContext();
	}

}
