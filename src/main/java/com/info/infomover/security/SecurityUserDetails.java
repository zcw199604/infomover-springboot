package com.info.infomover.security;

import java.util.Collection;

import org.springframework.security.core.GrantedAuthority;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SecurityUserDetails extends org.springframework.security.core.userdetails.User {
	private static final long serialVersionUID = -6420028585939256234L;

	private Long userId;

	public SecurityUserDetails(Long userId, String username, String password,
			Collection<? extends GrantedAuthority> authorities) {
		super(username, password, true, true, true, true, authorities);
		this.userId = userId;
	}

	public SecurityUserDetails(Long userId, String username, String password, boolean enabled, boolean accountNonExpired,
			boolean credentialsNonExpired, boolean accountNonLocked,
			Collection<? extends GrantedAuthority> authorities) {
		super(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
		this.userId = userId;
	}
}
