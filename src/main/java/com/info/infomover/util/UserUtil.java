package com.info.infomover.util;

import com.info.infomover.entity.User;
import org.slf4j.MDC;

/**
 * 用户工具类
 *
 * @author zhengcw on 2021/3/17
 */
public class UserUtil {

    private final static ThreadLocal<User> tlUser = new ThreadLocal<User>();

    public static final String KEY_LANG = "lang";

    public static final String KEY_USER = "user";

    public static void setUser(User user) {
        tlUser.set(user);

        // 把用户信息放到log4j
        MDC.put(KEY_USER, "[" + user.name +"]:");
    }

    /**
     * 如果没有登录，返回null
     *
     * @return
     */
    public static User getUserIfLogin() {
        return tlUser.get();
    }

    /**
     * 如果没有登录会抛出异常
     *
     * @return
     */
    public static User getUser() {
        User user = tlUser.get();
        if (user == null) {
            throw new UserNotLoginExecption("请先登录");
        }
        return user;
    }

    public static String getUserRole() {
        return getUser().getRole();
    }


    public static Long getUserId() {
        return getUser().getId();
    }

    public static String getUserName() {
        return getUser().getName();
    }

    public static String getChineseName() {
        return getUser().getChineseName();
    }

    public static void clearAllUserInfo() {
        tlUser.remove();

        MDC.remove(KEY_USER);
    }

}
