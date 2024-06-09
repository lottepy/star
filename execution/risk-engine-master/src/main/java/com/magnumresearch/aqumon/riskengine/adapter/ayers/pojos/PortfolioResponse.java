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
public class PortfolioResponse {
    @XmlAttribute(name = "type")                private String type;
    @XmlAttribute(name = "msgnum")              private String msgnum;
    @XmlElement(name = "client_acc_code")       private String clientAccCode;
    @XmlElement(name = "total_cash")            private BigDecimal totalCash;
    @XmlElement(name = "total_avail_cash")      private BigDecimal totalAvailCash;
    @XmlElement(name = "total_uncleared_cash")  private BigDecimal totalUnclearedCash;
    @XmlElement(name = "total_prev_mkt_value")  private BigDecimal totalPrevMktValue;
    @XmlElement(name = "os_buy")                private BigDecimal osBuy;
    @XmlElement(name = "used_pp")               private BigDecimal usedPP;
    @XmlElement(name = "avail_prev_pp")         private BigDecimal availPrevPP;
    @XmlElement(name = "avail_prev_pp_e")       private BigDecimal availPrevPPE;
    @XmlElement(name = "limits")                private Limits limits;
    @XmlElement(name = "cash_pos")              private CashPos cashPos;
    @XmlElement(name = "product_pos")           private ProductPos productPos;
}
