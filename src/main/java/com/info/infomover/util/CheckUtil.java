package com.info.infomover.util;

public class CheckUtil {
    public static void checkTrue(boolean flag, String errorMsg) {
        if (flag) {
            throw new RuntimeException(errorMsg);
        }
    }
}
