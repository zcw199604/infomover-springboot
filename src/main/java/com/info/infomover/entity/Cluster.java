package com.info.infomover.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Entity
@Table(name = "infomover_cluster")
public class Cluster extends AbstractPersistable<Long> implements Serializable {
    private static final long serialVersionUID = 3791258555311291521L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    @NotBlank
    @Column(unique = true)
    public String name;

    @Column(name = "connector_url", length = 512)
    public String connectorUrl;

    @Column(name = "broker_list")
    public String brokerList;

    @Column(name = "security_protocol")
    public String securityProtocol;

    @Column(name = "sasl_mechanism")
    public String saslMechanism;

    //@Username
    public String username;
    //@Password
    public String password;
    @Column(name = "service_name")
    public String serviceName;
    @Column(name = "login_module")
    public String loginModule;

    public Integer enabled;

    public String note;

    @Column(name = "creator_id")
    public Long creatorId;

    @Column(name = "creator_chinese_name")
    public String creatorChineseName;

    @Column(name = "last_modifier")
    public String lastModifier;

    @Column(name = "create_time", columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;

    @Column(name = "update_time", columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;

    @Transient
    public List<ConnectDistribute> connectDistribute;

    public String initSaslJaasConfig() {
        String str = "%s required username='%s' password='%s';";
        String saslJaasConfig = String.format(str, loginModule, username, password);
        return saslJaasConfig;
    }

    public String initSecurityProtocolDoc() {
        String str = "%s { %s required username='%s' password='%s'; };";
        String saslJaasConfig = String.format(str, serviceName, loginModule, username, password);
        return saslJaasConfig;
    }
}
