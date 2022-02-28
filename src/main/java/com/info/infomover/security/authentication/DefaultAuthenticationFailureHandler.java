package com.info.infomover.security.authentication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
	private final ObjectMapper objectMapper;

	public DefaultAuthenticationFailureHandler(final ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException e) throws IOException, ServletException {
		response.setStatus(HttpStatus.UNAUTHORIZED.value());
		response.setContentType(MediaType.APPLICATION_JSON_VALUE);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("status", HttpStatus.UNAUTHORIZED.value());
		map.put("message", e.getMessage());
		objectMapper.writeValue(response.getWriter(), map);
	}
}
