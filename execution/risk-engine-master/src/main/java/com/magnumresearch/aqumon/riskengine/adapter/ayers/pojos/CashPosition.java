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
public class CashPosition {
    @XmlElement(name = "ccy")               private String ccy;
    @XmlElement(name = "amt")               private BigDecimal amt;
    @XmlElement(name = "avail_amt")         private BigDecimal availAmt;
    @XmlElement(name = "uncleared_amt")     private BigDecimal unclearedAmt;
}
