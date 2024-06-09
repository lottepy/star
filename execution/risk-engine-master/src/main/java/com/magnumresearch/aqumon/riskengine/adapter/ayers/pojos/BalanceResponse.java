package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.*;
import java.math.BigDecimal;
import java.util.List;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class BalanceResponse {
    @XmlAttribute(name = "type")            private String type;
    @XmlAttribute(name = "msgnum")          private String msgnum;
    @XmlElement(name = "client_acc_code")   private String clientAccCode;
    @XmlElement(name = "cash")              private BigDecimal cash;
    @XmlElement(name = "mv")                private BigDecimal marketeValue;
    @XmlElement(name = "cash_on_hold")      private BigDecimal cashOnHold;
    @XmlElement(name = "net_unsett_amt")    private BigDecimal netUnsettAmount;
    @XmlElement(name = "unsett_buy_amt")    private BigDecimal unsettBuy_Amount;
    @XmlElement(name = "unsett_sell_amt")   private BigDecimal unsettSellAmount;
    @XmlElement(name = "db_int")            private BigDecimal dbInt;
    @XmlElement(name = "crInt")             private BigDecimal crInt;
    @XmlElement(name = "cash_withdrawal")   private BigDecimal cashIithdrawal;
    @XmlElement(name = "buy_pwr")           private BigDecimal buyPower;

    @XmlElementWrapper(name="bals")
    @XmlElement(name="bal")                 private List<Balance> balances;
}
