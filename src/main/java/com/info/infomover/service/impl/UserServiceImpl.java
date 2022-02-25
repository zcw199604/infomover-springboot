package com.info.infomover.service.impl;

import com.info.infomover.entity.User;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.service.UserService;
import com.info.infomover.util.BCryptPasswordEncoderUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service("infomover_user_service")
public class UserServiceImpl implements UserService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private BCryptPasswordEncoderUtil bcryptHash;

    @Override
    public boolean verifyBCryptPassword(String rawPassword, String encodedPassword) throws Exception  {
        return bcryptHash.matches(rawPassword,encodedPassword);
    }

    @Override
    @Transactional
    public void enableUser(Long userId, boolean enabled) {
        User user = userRepository.findById(userId).get();
        if (user != null) {
            if (enabled) {
                user.setStatus(User.Status.ENABLED);
            } else {
                user.setStatus(User.Status.DISABLED);
            }
            userRepository.saveAndFlush(user);
        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

    @Override
    @Transactional
    public void enableAdmin(Long userId, boolean enabled) {
        User user = userRepository.findById(userId).get();
        if (user != null) {
            if (enabled) {
                user.setRole(User.Role.Admin.name());
            } else {
                user.setRole(User.Role.User.name());
            }
            userRepository.saveAndFlush(user);
        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

    @Override
    @Transactional
    public void updateEmail(String curUserName, Long userId, String newEmail) {
        User user = userRepository.findById(userId).get();
        if (user != null) {
            if (!user.name.equals(curUserName)) {
                throw new RuntimeException("wrong user");
            }
            user.setEmail(newEmail);
            userRepository.saveAndFlush(user);
        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

    @Override
    public void updatePassword(String curUserName, Long userId, String oldPwd, String newPwd) {
        User user = userRepository.findById(userId).get();
        if (user != null) {
            if (!user.name.equals(curUserName)) {
                throw new RuntimeException("wrong user");
            }
            boolean validated = false;
            try {
                validated = verifyBCryptPassword(oldPwd,user.password);
            } catch (Exception e) {
                log.error("verifyBCryptPassword error", e);
            }

            if (validated) {
                user.setPassword(bcryptHash.encode(newPwd));
                userRepository.saveAndFlush(user);
            } else {
                throw new RuntimeException("wrong old password");
            }

        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

    @Override
    public void add(String username, String chineseName, String password, String role, String email, String note) {
        User user = new User();
        user.name = username;
        user.chineseName = chineseName;
        user.password = bcryptHash.encode(password);
        user.role = role;
        user.email = email;
        user.status = User.Status.NEW;
        user.createTime = LocalDateTime.now();
        user.note = note;
        userRepository.save(user);
    }
}
