package com.info.infomover.security.authentication.validate;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.util.matcher.RequestMatcher;

import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.TokenAuthenticationProcessingFilter;

public class TokenValidateAuthenticationProcessingFilter extends TokenAuthenticationProcessingFilter {

	public TokenValidateAuthenticationProcessingFilter(RequestMatcher matcher,
			final AuthenticationFailureHandler failureHandler, final JwtTokenGenerater jwtTokenGenerater) {
		super(matcher, failureHandler, jwtTokenGenerater);
	}

	@Override
	protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, FilterChain chain,
			Authentication authResult) throws IOException, ServletException {
		SecurityContext context = SecurityContextHolder.createEmptyContext();
		context.setAuthentication(authResult);
		SecurityContextHolder.setContext(context);
		chain.doFilter(request, response);
	}
}
