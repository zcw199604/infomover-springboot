package com.info.infomover.security.exceptions;

import org.springframework.security.authentication.AuthenticationServiceException;

public class AuthMethodNotSupportException extends AuthenticationServiceException {
	private static final long serialVersionUID = 3705043083010304496L;

	public AuthMethodNotSupportException(String msg) {
		super(msg);
	}
}
