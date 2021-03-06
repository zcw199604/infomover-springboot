package com.info.infomover.security.exceptions;

import org.springframework.security.authentication.AuthenticationServiceException;

public class ForbiddenException extends AuthenticationServiceException {
	private static final long serialVersionUID = 5869204524357172888L;

	private String unauthorizedUrl;

	public ForbiddenException(String msg) {
		super(msg);
	}

	public ForbiddenException(String msg, Throwable t) {
		super(msg, t);
	}

	public ForbiddenException(String msg, String unauthorizedUrl) {
		super(msg);
		this.unauthorizedUrl = unauthorizedUrl;
	}

	public ForbiddenException(String msg, String unauthorizedUrl, Throwable t) {
		super(msg, t);
		this.unauthorizedUrl = unauthorizedUrl;
	}

	public String getUnauthorizedUrl() {
		return unauthorizedUrl;
	}

	public void setUnauthorizedUrl(String unauthorizedUrl) {
		this.unauthorizedUrl = unauthorizedUrl;
	}
}
