package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.*;
import java.math.BigDecimal;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class CashIONotification {
    @XmlAttribute(name = "type")            private String type;
    @XmlAttribute(name = "msgnum")          private String msgnum;
    @XmlElement(name = "client_acc_code")   private String clientAccCode;
    @XmlElement(name = "ccy")               private String ccy;
    @XmlElement(name = "amount")            private BigDecimal amount;
    @XmlElement(name = "uncleared_amt")     private BigDecimal unclearedAmt;
    @XmlElement(name = "rate")              private BigDecimal rate;
    @XmlElement(name = "tran_type")         private String tranType;
}
