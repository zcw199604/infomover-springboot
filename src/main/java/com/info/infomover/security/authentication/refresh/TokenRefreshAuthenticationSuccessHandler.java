package com.info.infomover.security.authentication.refresh;

import org.springframework.security.core.Authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.info.infomover.security.SecurityUserDetails;
import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.TokenGenerationAuthenticationSuccessHandler;
import com.info.infomover.security.authentication.validate.TokenValidateAuthenticationToken;

public class TokenRefreshAuthenticationSuccessHandler extends TokenGenerationAuthenticationSuccessHandler {

	public TokenRefreshAuthenticationSuccessHandler(final ObjectMapper objectMapper, final JwtTokenGenerater jwtTokenGenerater) {
		super(objectMapper, jwtTokenGenerater);
	}

	@Override
	public SecurityUserDetails extractedSecurityUserDetails(Authentication authentication) {
		TokenValidateAuthenticationToken auth = (TokenValidateAuthenticationToken) authentication;
		SecurityUserDetails userDetails = (SecurityUserDetails) auth.getPrincipal();
		return userDetails;
	}
}
