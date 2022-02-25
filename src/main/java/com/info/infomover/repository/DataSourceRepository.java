package com.info.infomover.repository;

import com.info.infomover.entity.DataSource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataSourceRepository extends JpaRepository<DataSource, Long> {

    /*@Transactional
    public DataSource findDatasourceById(long id) {
        log.info("Find datasource {}", id);
        DataSource dataSource = findById(id);
        return dataSource;
    }*/

    public DataSource findByName(String name);
}
