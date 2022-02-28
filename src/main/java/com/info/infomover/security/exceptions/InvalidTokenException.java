package com.info.infomover.security.exceptions;

public class InvalidTokenException extends TokenException {
	private static final long serialVersionUID = -294671188037098603L;

	public InvalidTokenException(String msg) {
		super(msg);
	}

	public InvalidTokenException(String msg, Throwable t) {
		super(msg, t);
	}

	public InvalidTokenException(String msg, String token) {
		super(msg, token);
	}

	public InvalidTokenException(String msg, String token, Throwable t) {
		super(msg, token, t);
	}

}
