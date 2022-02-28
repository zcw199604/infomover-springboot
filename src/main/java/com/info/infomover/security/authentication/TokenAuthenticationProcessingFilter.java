package com.info.infomover.security.authentication;

import com.info.infomover.security.SecurityProperties;
import com.info.infomover.security.SecurityUserDetails;
import com.info.infomover.security.authentication.validate.TokenValidateAuthenticationToken;
import com.info.infomover.security.exceptions.InvalidTokenException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.util.matcher.RequestMatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public abstract class TokenAuthenticationProcessingFilter extends AbstractAuthenticationProcessingFilter {
	@Setter
	protected String tokenHeaderName = SecurityProperties.TOKEN_HEADER_NAME;
	@Setter
	protected String tokenParamName = SecurityProperties.TOKEN_PARAM_NAME;

	protected final AuthenticationFailureHandler failureHandler;
	protected final JwtTokenGenerater jwtTokenGenerater;

	public TokenAuthenticationProcessingFilter(RequestMatcher matcher,
			final AuthenticationFailureHandler failureHandler, final JwtTokenGenerater jwtTokenGenerater) {
		super(matcher);
		this.failureHandler = failureHandler;
		this.jwtTokenGenerater = jwtTokenGenerater;
	}

	@Override
	public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
			throws AuthenticationException, IOException, ServletException {
		String tokenPayload = request.getHeader(tokenHeaderName);
		if (log.isTraceEnabled()) {
			log.trace(String.format("Processing request '%s' with header token '%s' ", request.getServletPath(),
					tokenPayload));
		}

		if (StringUtils.isEmpty(tokenPayload)) {
			tokenPayload = request.getParameter(tokenParamName);
			if (log.isTraceEnabled()) {
				log.trace(String.format("Processing request '%s' with parameter token '%s' ", request.getServletPath(),
						tokenPayload));
			}
		}

		if (StringUtils.isEmpty(tokenPayload)) {
			throw new InvalidTokenException("Invalid token: empty token value !");
		}

		if (tokenPayload.startsWith("Bearer ")) {
			tokenPayload = tokenPayload.replace("Bearer ", "");
		}

		SecurityUserDetails myUserDetails = jwtTokenGenerater.parseToken(tokenPayload);
		return getAuthenticationManager().authenticate(new TokenValidateAuthenticationToken(myUserDetails,
				myUserDetails.getPassword(), myUserDetails.getAuthorities()));
	}

	@Override
	protected void unsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException failed) throws IOException, ServletException {
		SecurityContextHolder.clearContext();
		failureHandler.onAuthenticationFailure(request, response, failed);
	}
}
