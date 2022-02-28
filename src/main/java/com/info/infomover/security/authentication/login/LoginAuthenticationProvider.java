package com.info.infomover.security.authentication.login;

import com.info.infomover.util.AesUtils;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.util.Assert;

public class LoginAuthenticationProvider implements AuthenticationProvider {
	private final UserDetailsService userDetailsService;
	private final PasswordEncoder passwordEncoder;

	public LoginAuthenticationProvider(final UserDetailsService userDetailsService,
			final PasswordEncoder passwordEncoder) {
		this.userDetailsService = userDetailsService;
		this.passwordEncoder = passwordEncoder;
	}

	@Override
	public Authentication authenticate(Authentication authentication) throws AuthenticationException {
		Assert.notNull(authentication, "No authentication data provided");
		UsernamePasswordAuthenticationToken auth = (UsernamePasswordAuthenticationToken) authentication;

		// 用户认证
		String username = (String) auth.getPrincipal();
		String password = (String) auth.getCredentials();
		// 加载用户信息
		UserDetails userDetails = userDetailsService.loadUserByUsername(username);
		// 用户信息认证
		if (!passwordEncoder.matches(AesUtils.wrappeDaesDecrypt(password), userDetails.getPassword())) {
			throw new BadCredentialsException("Authentication Failed. Username or Password not valid.");
		}
		// 认证通过
		return new LoginUsernamePasswordAuthenticationToken(userDetails, userDetails.getPassword(),
				userDetails.getAuthorities());
	}

	@Override
	public boolean supports(Class<?> authentication) {
		return (LoginUsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication));
	}
}
