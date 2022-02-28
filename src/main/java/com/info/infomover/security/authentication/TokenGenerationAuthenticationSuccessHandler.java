package com.info.infomover.security.authentication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.WebAttributes;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.info.infomover.security.SecurityUserDetails;

public abstract class TokenGenerationAuthenticationSuccessHandler implements AuthenticationSuccessHandler {

	private final ObjectMapper objectMapper;
	private final JwtTokenGenerater jwtTokenGenerater;

	public TokenGenerationAuthenticationSuccessHandler(final ObjectMapper objectMapper,
			final JwtTokenGenerater jwtTokenGenerater) {
		this.objectMapper = objectMapper;
		this.jwtTokenGenerater = jwtTokenGenerater;
	}

	@Override
	public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
			Authentication authentication) throws IOException, ServletException {
		SecurityUserDetails userDetails = extractedSecurityUserDetails(authentication);
		String token = jwtTokenGenerater.generateToken(userDetails);

		// 返回报文
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("token", token);
		map.put("role", userDetails.getAuthorities());
		map.put("id", userDetails.getUserId());

		// 将token返回给客户端
		response.setStatus(HttpStatus.OK.value());
		response.setContentType(MediaType.APPLICATION_JSON_VALUE);
		objectMapper.writeValue(response.getWriter(), map);
		clearAuthenticationAttributes(request);
	}

	protected abstract SecurityUserDetails extractedSecurityUserDetails(Authentication authentication);

	/**
	 * Removes temporary authentication-related data which may have been stored in
	 * the session during the authentication process..
	 */
	protected final void clearAuthenticationAttributes(HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		if (session == null) {
			return;
		}
		session.removeAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
	}
}
