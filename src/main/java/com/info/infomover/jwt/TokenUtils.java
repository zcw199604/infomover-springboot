package com.info.infomover.jwt;

import com.info.infomover.entity.User;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

public class TokenUtils {

    private static String secret = "abcdefghijklmnopqrstuvwxyz";

    public static String generateToken(String username, Set<String> roles, Long duration, String issuer) throws Exception {
        //JwtClaimsBuilder claimsBuilder = Jwt.claims();
        long currentTimeInSecs = currentTimeInSecs();
        Set<String> groups = new HashSet<>();
        for (String role : roles) groups.add(role);
/*



        claimsBuilder.issuer(issuer);
        claimsBuilder.subject(username);
        claimsBuilder.issuedAt(currentTimeInSecs);
        claimsBuilder.expiresAt(currentTimeInSecs + duration);
        claimsBuilder.groups(groups);
*/

        return Jwts.builder()
                .setSubject(username)
                .setIssuer(issuer)
                .claim("roles", groups)
                .setIssuedAt(new Date(currentTimeInSecs * 1000))
                .setExpiration(new Date(currentTimeInSecs * 1000 + duration * 1000)) /*过期时间*/
                .signWith(SignatureAlgorithm.HS256, secret)
                .compact();
    }


    public static PrivateKey readPrivateKey(final String pemResName) throws Exception {
        try (InputStream contentIS = TokenUtils.class.getResourceAsStream(pemResName)) {
            byte[] tmp = new byte[4096];
            int length = contentIS.read(tmp);
            return decodePrivateKey(new String(tmp, 0, length, "UTF-8"));
        }
    }

    public static PrivateKey decodePrivateKey(final String pemEncoded) throws Exception {
        byte[] encodedBytes = toEncodedBytes(pemEncoded);

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encodedBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    public static byte[] toEncodedBytes(final String pemEncoded) {
        final String normalizedPem = removeBeginEnd(pemEncoded);
        return Base64.getDecoder().decode(normalizedPem);
    }

    public static String removeBeginEnd(String pem) {
        pem = pem.replaceAll("-----BEGIN (.*)-----", "");
        pem = pem.replaceAll("-----END (.*)----", "");
        pem = pem.replaceAll("\r\n", "");
        pem = pem.replaceAll("\n", "");
        return pem.trim();
    }

    public static int currentTimeInSecs() {
        long currentTimeMS = System.currentTimeMillis();
        return (int) (currentTimeMS / 1000);
    }

    public static Claims checkToken(String token){
        try {

            final Claims claims = Jwts.parser().setSigningKey(secret).parseClaimsJws(token).getBody();
            return claims;
        } catch (ExpiredJwtException e1) {
            throw new RuntimeException("登录信息过期，请重新登录");
        } catch (Exception e) {
            throw new RuntimeException("用户未登录，请重新登录");
        }
    }

    /**
     * 获取请求token
     *
     * @param request
     * @return token
     */
    private static String getToken(HttpServletRequest request)
    {
        String token = request.getHeader("Authorization");
        if (StringUtils.isNotEmpty(token) && token.startsWith("Bearer "))
        {
            token = token.replace("Bearer ", "");
        }
        return token;
    }

    /**
     * 获取用户身份信息
     *
     * @return 用户信息
     */
    public static User getLoginUser(HttpServletRequest request)
    {
        // 获取请求携带的令牌
        String token =  getToken(request);
        if (StringUtils.isNotEmpty(token))
        {
            Claims claims = checkToken(token);
            // 解析对应的权限以及用户信息
            String userName = claims.getSubject();
            User user = new User();
            // TODO 以后可能会携带用户权限
            user.setName(userName);
            if (claims.get("roles") != null && claims.get("roles") instanceof List) {
                List<String> list = (List) claims.get("roles");
                user.setRole(list.get(0));
            }
            return user;
        }
        return null;
    }
}