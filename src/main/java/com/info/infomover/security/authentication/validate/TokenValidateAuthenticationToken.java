package com.info.infomover.security.authentication.validate;

import java.util.Collection;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

public class TokenValidateAuthenticationToken extends UsernamePasswordAuthenticationToken {
	private static final long serialVersionUID = 3652648388316224728L;

	public TokenValidateAuthenticationToken(Object principal, Object credentials) {
		super(principal, credentials);
	}

	public TokenValidateAuthenticationToken(Object principal, Object credentials,
			Collection<? extends GrantedAuthority> authorities) {
		super(principal, credentials, authorities);
	}
}
