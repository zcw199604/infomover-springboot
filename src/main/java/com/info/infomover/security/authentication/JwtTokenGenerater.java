package com.info.infomover.security.authentication;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Sets;
import com.info.infomover.security.SecurityUserDetails;
import com.info.infomover.security.SecurityProperties.JwtProperties;
import com.info.infomover.security.exceptions.InvalidTokenException;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;

public class JwtTokenGenerater {

	private final Set<String> scopes = Sets.newHashSet("read", "write", "trust");
	private final JwtProperties jwtProperties;

	public JwtTokenGenerater(JwtProperties jwtProperties) {
		this.jwtProperties = jwtProperties;
	}

	public String generateToken(SecurityUserDetails userDetails) {
		String subject = userDetails.getUsername();
		Collection<? extends GrantedAuthority> authorities = userDetails.getAuthorities();

		// 设置token信息
		Claims claims = Jwts.claims();
		claims.put("userId", userDetails.getUserId());
		claims.put("scopes", scopes);
		claims.put("authorities", authorities.stream().map(t -> t.getAuthority()).collect(Collectors.joining(";")));

		// 签章
		LocalDateTime currentTime = LocalDateTime.now();
		return Jwts.builder()//
				.setId(UUID.randomUUID().toString())//
				.setClaims(claims)//
				.setSubject(subject)//
				.setIssuer(jwtProperties.getTokenIssuer())//
				.setIssuedAt(Date.from(currentTime.atZone(ZoneId.systemDefault()).toInstant()))//
				.setExpiration(Date.from(currentTime.plusSeconds(jwtProperties.getAccessTokenExpIn())
						.atZone(ZoneId.systemDefault()).toInstant()))//
				.signWith(SignatureAlgorithm.HS512, jwtProperties.getTokenSigningKey())//
				.compact();
	}

	public SecurityUserDetails parseToken(String token) {
		Jws<Claims> parseClaims = parseClaims(jwtProperties.getTokenSigningKey(), token);
		Claims body = parseClaims.getBody();
		String subject = body.getSubject();
		long userId = Long.parseLong(body.get("userId").toString());
		String object = (String) body.get("authorities");
		return new SecurityUserDetails(userId, subject, "******",
				Arrays.stream(object.split(";")).map(t -> new SimpleGrantedAuthority(t)).collect(Collectors.toList()));
	}

	private Jws<Claims> parseClaims(String signingKey, String token) {
		try {
			return Jwts.parser().setSigningKey(signingKey).parseClaimsJws(token);
		} catch (UnsupportedJwtException | MalformedJwtException | IllegalArgumentException | SignatureException
				| ExpiredJwtException e) {
			throw new InvalidTokenException(String.format("Invalid JWT token '%s'", token), e);
		}
	}
}
