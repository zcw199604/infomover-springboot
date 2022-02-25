package com.info.infomover.repository;

import com.info.infomover.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    User findByName(String name);

    long countByChineseName(String chineseName);
}
