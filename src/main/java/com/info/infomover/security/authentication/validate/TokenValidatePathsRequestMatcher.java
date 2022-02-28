package com.info.infomover.security.authentication.validate;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.Assert;

public class TokenValidatePathsRequestMatcher implements RequestMatcher {
	private OrRequestMatcher matchers;
	private OrRequestMatcher processingMatchers;

	public TokenValidatePathsRequestMatcher(String[] skipPaths, String[] processingPaths) {
		Assert.notNull(skipPaths, "Parameter 'skipPaths' must be not null !");
		Assert.notNull(processingPaths, "Parameter 'processingPaths' must be not null !");
		matchers = new OrRequestMatcher(
				Arrays.stream(skipPaths).map(path -> new AntPathRequestMatcher(path)).collect(Collectors.toList()));
		processingMatchers = new OrRequestMatcher(Arrays.stream(processingPaths)
				.map(path -> new AntPathRequestMatcher(path)).collect(Collectors.toList()));
	}

	@Override
	public boolean matches(HttpServletRequest request) {
		if (matchers.matches(request)) {
			return false;
		}
		return processingMatchers.matches(request);
	}
}
