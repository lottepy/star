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
@Getter
@Setter
@ToString
public class Trade {
    @XmlElement(name = "trade_id")          private String tradeId;
    @XmlElement(name = "trade_date")        private String tradeDate;
    @XmlElement(name = "price")             private BigDecimal price;
    @XmlElement(name = "qty")               private int qty;
    @XmlElement(name = "create_time")       private String createTime;
    @XmlElement(name = "exch_trade_ref")    private String exchTradeRef;
    @XmlElement(name = "charge")            private String charge;
}
