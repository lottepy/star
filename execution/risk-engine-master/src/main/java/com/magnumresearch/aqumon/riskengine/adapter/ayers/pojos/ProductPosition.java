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
public class ProductPosition {
    @XmlElement(name = "exchange_code")     private String exchangeCode;
    @XmlElement(name = "product_code")      private String productCode;
    @XmlElement(name = "product_name")      private String productName;
    @XmlElement(name = "qty")               private BigDecimal qty;
    @XmlElement(name = "avail_qty")         private BigDecimal availQty;
    @XmlElement(name = "uncleared_qty")     private BigDecimal unclearedQty;
    @XmlElement(name = "ccy")               private String ccy;
    @XmlElement(name = "avg_cost")          private BigDecimal avgCost;
    @XmlElement(name = "loan_percent")      private BigDecimal loanPercent;
}
