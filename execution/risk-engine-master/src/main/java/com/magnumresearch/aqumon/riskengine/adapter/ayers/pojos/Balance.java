package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.math.BigDecimal;

@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
@Getter @Setter @ToString
public class Balance {
    @XmlElement(name = "ccy")               private String ccy;
    @XmlElement(name = "cash")              private BigDecimal cash;
    @XmlElement(name = "mv")                private BigDecimal mv;
    @XmlElement(name = "cash_on_hold")      private BigDecimal cashOnHold;
    @XmlElement(name = "net_unsett_amt")    private BigDecimal netUnsettAmount;
    @XmlElement(name = "unsett_buy_amt")    private BigDecimal unsettBuy_Amount;
    @XmlElement(name = "unsett_sell_amt")   private BigDecimal unsettSellAmount;
    @XmlElement(name = "db_int")            private BigDecimal dbInt;
    @XmlElement(name = "crInt")             private BigDecimal crInt;
    @XmlElement(name = "cash_withdraw")     private BigDecimal cashWithdraw;
    @XmlElement(name = "buy_pwr")           private BigDecimal buyPower;
}
