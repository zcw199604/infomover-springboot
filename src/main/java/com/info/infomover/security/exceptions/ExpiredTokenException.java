package com.info.infomover.security.exceptions;

public class ExpiredTokenException extends TokenException {

	private static final long serialVersionUID = -5959543783324224864L;

	public ExpiredTokenException(String msg) {
		super(msg);
	}

	public ExpiredTokenException(String msg, Throwable t) {
		super(msg, t);
	}

	public ExpiredTokenException(String msg, String token) {
		super(msg, token);
	}

	public ExpiredTokenException(String msg, String token, Throwable t) {
		super(msg, token, t);
	}

}