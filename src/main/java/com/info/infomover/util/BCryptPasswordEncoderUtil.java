package com.info.infomover.util;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class BCryptPasswordEncoderUtil extends BCryptPasswordEncoder {

    @Override
    public String encode(CharSequence rawPassword) {
        return super.encode(rawPassword);
    }

    @Override
    public boolean matches(CharSequence rawPassword, String encodedPassword) {

        return super.matches(rawPassword,encodedPassword);
    }


    public static void main(String[] args) {
        System.out.println(new BCryptPasswordEncoderUtil().encode("123456"));
    }
}