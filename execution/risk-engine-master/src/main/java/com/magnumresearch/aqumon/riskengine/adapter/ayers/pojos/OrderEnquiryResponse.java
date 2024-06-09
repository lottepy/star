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
public class OrderEnquiryResponse {
    @XmlAttribute(name = "type")                    private String type;
    @XmlAttribute(name = "msgnum")                  private String msgnum;
    @XmlElement(name = "reference")                 private String reference;
    @XmlElement(name = "reference1")                private String reference1;
    @XmlElement(name = "trades")                    private Trades trades;
    @XmlElement(name = "updated_time")              private String updatedTime;
    @XmlElement(name = "create_time")               private String createTime;
    @XmlElement(name = "price")                     private BigDecimal price;
    @XmlElement(name = "qty")                       private BigDecimal qty;
    @XmlElement(name = "outstand_qty")              private BigDecimal outstandQty;
    @XmlElement(name = "exec_qty")                  private BigDecimal execQty;
    @XmlElement(name = "exec_price")                private BigDecimal execPrice;
    @XmlElement(name = "order_status")              private String orderStatus;
    @XmlElement(name = "order_sub_status")          private String orderSubStatus;
    @XmlElement(name = "order_no")                  private String orderNo;
    @XmlElement(name = "reject_reason")             private String rejectReason;
    @XmlElement(name = "last_order_action_code")    private String lastOrderActionCode;
    @XmlElement(name = "bs_flag")                   private String bsFlag;
    @XmlElement(name = "client_acc_code")           private String clientAccCode;
    @XmlElement(name = "exchange_code")             private String exchangeCode;
    @XmlElement(name = "product_code")              private String productCode;
    @XmlElement(name = "order_type")                private String orderType;
    @XmlElement(name = "input_channel")             private String inputChannel;
    @XmlAnyElement(lax = true)                      private List<Object> anything;
}
