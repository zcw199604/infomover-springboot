package com.info.infomover.security.exceptions;

import org.springframework.security.core.AuthenticationException;

public class TokenException extends AuthenticationException {
	private static final long serialVersionUID = -2711776825981689462L;

	protected String token;

	public TokenException(String msg) {
		super(msg);
	}

	public TokenException(String msg, Throwable t) {
		super(msg, t);
	}

	public TokenException(String msg, String token) {
		this(msg);
		this.token = token;
	}

	public TokenException(String msg, String token, Throwable t) {
		super(msg, t);
		this.token = token;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

}
