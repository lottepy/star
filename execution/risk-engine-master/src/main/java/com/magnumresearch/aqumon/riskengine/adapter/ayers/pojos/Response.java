package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.*;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class Response {
    @XmlAttribute(name = "type")                private String type;
    @XmlAttribute(name = "msgnum")              private String msgnum;
    @XmlElement(name = "status")                private int status;
    @XmlElement(name = "error")                 private String error;
    @XmlElement(name = "errorCode")             private String errorCode;
    @XmlElement(name = "information")           private String information;
    @XmlElement(name = "alert_change_pwd")      private String alertChangePwd;
    @XmlElement(name = "force_change_pwd")      private String forceChangePwd;
    @XmlElement(name = "pwd_expiry_date")       private String pwdExpiryDate;
    @XmlElement(name = "last_login_time")       private String lastLoginTime;
    @XmlElement(name = "require_activation")    private String requireActivation;
    @XmlElement(name = "recovery_order_count")  private String recoveryOrderCount;
}
