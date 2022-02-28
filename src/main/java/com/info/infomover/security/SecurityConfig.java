package com.info.infomover.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import com.info.infomover.security.SecurityProperties.EntrypointsProperties;
import com.info.infomover.security.authentication.login.LoginAuthenticationProvider;
import com.info.infomover.security.authentication.JwtTokenGenerater;
import com.info.infomover.security.authentication.login.LoginAuthenticationProcessingFilter;
import com.info.infomover.security.authentication.refresh.TokenRefreshAuthenticationProcessingFilter;
import com.info.infomover.security.authentication.validate.TokenValidateAuthenticationProcessingFilter;
import com.info.infomover.security.authentication.validate.TokenValidateAuthenticationProvider;
import com.info.infomover.security.authentication.validate.TokenValidatePathsRequestMatcher;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class SecurityConfig extends WebSecurityConfigurerAdapter {
	@Autowired
	private SecurityProperties securityProperties;
	@Autowired
	private PasswordEncoder passwordEncoder;
	@Autowired
	private LoginAuthenticationProvider loginAuthenticationProvider;
	@Autowired
	private AuthenticationSuccessHandler loginAuthenticationSuccessHandler;
	@Autowired
	private AuthenticationSuccessHandler tokenRefreshAuthenticationSuccessHandler;
	@Autowired
	private AuthenticationFailureHandler authenticationFailureHandler;
	@Autowired
	private JwtTokenGenerater jwtTokenGenerater;
	@Autowired
	private TokenValidateAuthenticationProvider tokenValidateAuthenticationProvider;

	private LoginAuthenticationProcessingFilter loginAuthenticationProcessingFilter() throws Exception {
		LoginAuthenticationProcessingFilter loginAuthenticationProcessingFilter = new LoginAuthenticationProcessingFilter(
				securityProperties.getEntrypoints().getTokenPath(), loginAuthenticationSuccessHandler,
				authenticationFailureHandler);
		loginAuthenticationProcessingFilter.setAuthenticationManager(authenticationManagerBean());
		return loginAuthenticationProcessingFilter;
	}

	private TokenValidateAuthenticationProcessingFilter tokenValidateAuthenticationProcessingFilter() throws Exception {
		EntrypointsProperties entrypoints = securityProperties.getEntrypoints();
		List<String> skipPaths = new ArrayList<String>();
		skipPaths.addAll(Arrays.asList(entrypoints.getTokenSkipPaths()));
		TokenValidatePathsRequestMatcher matcher = new TokenValidatePathsRequestMatcher(
				skipPaths.toArray(String[]::new), entrypoints.getAuthTokenPaths());
		TokenValidateAuthenticationProcessingFilter tokenValidateAuthenticationProcessingFilter = new TokenValidateAuthenticationProcessingFilter(
				matcher, authenticationFailureHandler, jwtTokenGenerater);
		tokenValidateAuthenticationProcessingFilter.setAuthenticationManager(authenticationManagerBean());

		if (StringUtils.isNotEmpty(entrypoints.getTokenHeaderName())) {
			tokenValidateAuthenticationProcessingFilter.setTokenHeaderName(entrypoints.getTokenHeaderName());
		}
		if (StringUtils.isNotEmpty(entrypoints.getTokenParamName())) {
			tokenValidateAuthenticationProcessingFilter.setTokenParamName(entrypoints.getTokenParamName());
		}
		return tokenValidateAuthenticationProcessingFilter;
	}

	private TokenRefreshAuthenticationProcessingFilter tokenRefreshAuthenticationProcessingFilter() throws Exception {
		EntrypointsProperties entrypoints = securityProperties.getEntrypoints();
		TokenRefreshAuthenticationProcessingFilter tokenRefreshAuthenticationProcessingFilter = new TokenRefreshAuthenticationProcessingFilter(
				entrypoints.getRefreshTokenPath(), tokenRefreshAuthenticationSuccessHandler,
				authenticationFailureHandler, jwtTokenGenerater);
		tokenRefreshAuthenticationProcessingFilter.setAuthenticationManager(authenticationManagerBean());

		if (StringUtils.isNotEmpty(entrypoints.getTokenHeaderName())) {
			tokenRefreshAuthenticationProcessingFilter.setTokenHeaderName(entrypoints.getTokenHeaderName());
		}
		if (StringUtils.isNotEmpty(entrypoints.getTokenParamName())) {
			tokenRefreshAuthenticationProcessingFilter.setTokenParamName(entrypoints.getTokenParamName());
		}
		return tokenRefreshAuthenticationProcessingFilter;
	}

	@Override
	protected void configure(HttpSecurity httpSecurity) throws Exception {
		httpSecurity//
				.cors().disable()//
				.csrf().disable()//
				.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)//
				.and().authorizeRequests()//
				.antMatchers(securityProperties.getEntrypoints().getTokenPath()).fullyAuthenticated() //
				.antMatchers(securityProperties.getWhiteList().getApiMethods()).permitAll()//
				.antMatchers(HttpMethod.OPTIONS).permitAll()//
				.anyRequest().authenticated();
		// 禁用缓存
		httpSecurity.headers().cacheControl().and().httpStrictTransportSecurity().disable();//
		httpSecurity.addFilterBefore(loginAuthenticationProcessingFilter(), UsernamePasswordAuthenticationFilter.class);
		httpSecurity.addFilterBefore(tokenRefreshAuthenticationProcessingFilter(),
				UsernamePasswordAuthenticationFilter.class);
		httpSecurity.addFilterBefore(tokenValidateAuthenticationProcessingFilter(),
				UsernamePasswordAuthenticationFilter.class);
	}

	@Override
	protected void configure(AuthenticationManagerBuilder auth) throws Exception {
		auth.userDetailsService(userDetailsService()).passwordEncoder(passwordEncoder);
		auth.authenticationProvider(loginAuthenticationProvider)
				.authenticationProvider(tokenValidateAuthenticationProvider);
	}

	@Bean
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
		return super.authenticationManagerBean();
	}

	@Override
	public void configure(WebSecurity web) throws Exception {
		web.ignoring().antMatchers(securityProperties.getWhiteList().getStaticResources());
	}
}