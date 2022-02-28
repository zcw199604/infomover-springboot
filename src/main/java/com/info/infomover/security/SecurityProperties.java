package com.info.infomover.security;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "security.token")
public class SecurityProperties {
	public static final String TOKEN_HEADER_NAME = "Authorization";// token头名称
	public static final String TOKEN_PARAM_NAME = "access_token";// token参数名称
	public static final String TOKEN_ENTRY_POINT = "/auth/token";// 获取token接口
	public static final String TOKEN_REFRESH_ENTRY_POINT = "/auth/refreshToken";// 刷新token接口
	public static final String TOKEN_REVOKE_ENTRY_POINT = "/auth/revokeToken";// 注销token接口
	public static final String TOKEN_AUTH_ENTRY_POINT = "/**";// 需要鉴权的路径

	/**
	 * 认证自定义端点
	 */
	@NestedConfigurationProperty
	private EntrypointsProperties entrypoints = new EntrypointsProperties();

	/**
	 * jwt配置
	 */
	@NestedConfigurationProperty
	private JwtProperties jwt = new JwtProperties();

	/**
	 * 认证白名单
	 */
	@NestedConfigurationProperty
	private WhiteListProperties whiteList = new WhiteListProperties();

	@Setter
	@Getter
	public static class EntrypointsProperties {

		/**
		 * token头名称
		 */
		private String tokenHeaderName = TOKEN_HEADER_NAME;

		/**
		 * token参数名称
		 */
		private String tokenParamName = TOKEN_PARAM_NAME;

		/**
		 * 获取token地址
		 */
		private String tokenPath = TOKEN_ENTRY_POINT;

		/**
		 * 刷新token地址
		 */
		private String refreshTokenPath = TOKEN_REFRESH_ENTRY_POINT;

		/**
		 * 销毁token地址
		 */
		private String revokeTokenPath = TOKEN_REVOKE_ENTRY_POINT;

		/**
		 * 需要认证的接口
		 */
		private String[] authTokenPaths = { TOKEN_AUTH_ENTRY_POINT };

		public String[] getTokenSkipPaths() {
			return new String[] { getTokenPath(), getRefreshTokenPath(), getRevokeTokenPath() };
		}
	}

	@Getter
	@Setter
	public static class JwtProperties {

		/**
		 * access_token有效期
		 */
		private Integer accessTokenExpIn = 7200;

		/**
		 * refresh_token有效期
		 */
		private Integer refreshTokenExpIn = 7200;

		/**
		 * token issuer
		 */
		private String tokenIssuer = "infomover";

		/**
		 * token signing key
		 */
		private String tokenSigningKey = "infomover";
	}

	@Setter
	@Getter
	public class WhiteListProperties {

		/**
		 * 静态资源
		 */
		private String[] staticResources = { "/", "/static/**", "/*.ico", "/*.icon", "/favicon.ico", "/index.html**",
				"/*.html", "/css/**", "/js/**", "/fonts/**", "/img/**", "/sub/**", "/sub/**", "/webjars/**" };

		/**
		 * 接口方法
		 */
		private String[] apiMethods = { "/", "/error" };

	}

}
