package com.info.infomover.entity;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.info.baymax.common.validation.constraints.Password;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "infomover_user")
public class User extends AbstractPersistable<Long> {
    private final static Logger logger = LoggerFactory.getLogger(User.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    @NotBlank
    public String name;

    @NotBlank
    @Column(unique = true)
    public String chineseName;

    @Password
    @Size(message = "不能少于6个字符", min = 6)
    @JsonIgnore
    public String password;

    @Email
    @NotBlank
    public String email;

    @Enumerated(EnumType.STRING)
    public Status status;

    public String role;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;

    public String note;

    public enum Role {
        Admin,
        User
    }

    public enum Status {
        NEW,
        PENDING,
        ENABLED,
        DISABLED,
    }

    /*public static User findByName(String name) {
        return find("name", name).firstResult();
    }

    public static void updatePassword(String curUserName, Long userId, String oldPwd, String newPwd) {
        User user = findById(userId);
        if (user != null) {
            if (!user.name.equals(curUserName)) {
                throw new RuntimeException("wrong user");
            }
            boolean validated = false;
            try {
                validated = User.verifyBCryptPassword(user.password, oldPwd);
            } catch (Exception e) {
                logger.error("verifyBCryptPassword error", e);
            }

            if (validated) {
                User.update("password = ?1 where id = ?2", BcryptUtil.bcryptHash(newPwd), userId);
            } else {
                throw new RuntimeException("wrong old password");
            }

        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

    public static void updateEmail(String curUserName, Long userId, String newEmail) {
        User user = findById(userId);
        if (user != null) {
            if (!user.name.equals(curUserName)) {
                throw new RuntimeException("wrong user");
            }
            User.update("email = ?1 where id = ?2", newEmail, userId);
        } else {
            throw new RuntimeException("user with id " + userId + " does not exist");
        }
    }

*/
}
