package com.info.infomover.service;

public interface UserService {
    /**
     * 判断密码是否相同
     *
     * @param rawPassword 真实密码
     * @param encodedPassword 加密后字符
     * @return 结果
     */
    boolean verifyBCryptPassword(String rawPassword, String encodedPassword) throws Exception;

    void enableUser(Long userId, boolean enabled);

    void enableAdmin(Long userId, boolean enabled);

    void updateEmail(String curUserName, Long userId, String newEmail);

    void updatePassword(String curUserName, Long userId, String oldPwd, String newPwd);

    void add(String username, String chineseName, String password, String role, String email, String note);
}
