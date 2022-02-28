package com.info.infomover.security.authentication.login;

import org.springframework.security.core.Authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.info.infomover.security.SecurityUserDetails;
import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.TokenGenerationAuthenticationSuccessHandler;

public class LoginAuthenticationSuccessHandler extends TokenGenerationAuthenticationSuccessHandler {
	public LoginAuthenticationSuccessHandler(ObjectMapper objectMapper, JwtTokenGenerater jwtTokenGenerater) {
		super(objectMapper, jwtTokenGenerater);
	}

	public SecurityUserDetails extractedSecurityUserDetails(Authentication authentication) {
		LoginUsernamePasswordAuthenticationToken auth = (LoginUsernamePasswordAuthenticationToken) authentication;
		SecurityUserDetails userDetails = (SecurityUserDetails) auth.getPrincipal();
		return userDetails;
	}

}
