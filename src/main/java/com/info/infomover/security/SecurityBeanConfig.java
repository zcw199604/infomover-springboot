package com.info.infomover.security;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.info.infomover.entity.User;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.security.authentication.DefaultAuthenticationFailureHandler;
import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.login.LoginAuthenticationProvider;
import com.info.infomover.security.authentication.login.LoginAuthenticationSuccessHandler;
import com.info.infomover.security.authentication.refresh.TokenRefreshAuthenticationSuccessHandler;

@Configuration
public class SecurityBeanConfig {
	@Bean
	public PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}

	@Bean
	public JwtTokenGenerater jwtTokenGenerater(SecurityProperties securityProperties) {
		return new JwtTokenGenerater(securityProperties.getJwt());
	}

	@Bean
	public AuthenticationFailureHandler authenticationFailureHandler(ObjectMapper objectMapper) {
		return new DefaultAuthenticationFailureHandler(objectMapper);
	}

	@Bean
	public LoginAuthenticationProvider loginAuthenticationProvider(UserDetailsService userDetailsService,
			PasswordEncoder passwordEncoder) {
		return new LoginAuthenticationProvider(userDetailsService, passwordEncoder);
	}

	@Bean
	public AuthenticationSuccessHandler loginAuthenticationSuccessHandler(ObjectMapper objectMapper,
			JwtTokenGenerater jwtTokenGenerater) {
		return new LoginAuthenticationSuccessHandler(objectMapper, jwtTokenGenerater);
	}

	@Bean
	public AuthenticationSuccessHandler tokenRefreshAuthenticationSuccessHandler(ObjectMapper objectMapper,
			JwtTokenGenerater jwtTokenGenerater) {
		return new TokenRefreshAuthenticationSuccessHandler(objectMapper, jwtTokenGenerater);
	}

	@Bean
	public UserDetailsService userDetailsService(UserRepository userRepository) {
		// 获取登录用户信息
		return new UserDetailsService() {
			@Override
			public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
				// 根据用户名获取用户
				User admin = userRepository.findByName(username);
				if (admin != null) {
					// 获取用户的所有权限
					// List<UmsPermission> permissionList =
					// adminService.getPermissionList(admin.getId());
					// 返回自己实现的用户用户信息*//*
					String role = admin.getRole();
					List<SimpleGrantedAuthority> authority = new ArrayList<>();
					authority.add(new SimpleGrantedAuthority(role));

					return new SecurityUserDetails(admin.getId(), admin.getName(), admin.getPassword(), authority);
				}
				throw new UsernameNotFoundException("用户名或密码错误");
			}
		};
	}
}
