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
public class OrderNotification {
    public static final String STATUS_CANCELLED = "CAN";
    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_REJECTED = "REJ";
    public static final String STATUS_PARTIALLY_FILLED = "PEX";
    public static final String STATUS_FULLY_FILLED = "FEX";
    public static final String STATUS_WAITING_FOR_APPROVAL = "WA";
    public static final String STATUS_SENDING_TO_EXCHANGE = "PRO";
    public static final String STATUS_QUEUED = "Q";
    @XmlAttribute(name = "type")                    private String type;
    @XmlElement(name = "reference")                 private String reference;
    @XmlElement(name = "reference1")                private String reference1;
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
    @XmlElement(name = "cr_action_code")            private String crActionCode;
    @XmlElement(name = "hold_release_condition")    private String holdReleaseCondition;
    @XmlElement(name = "last_history_id")           private String lastHistoryId;
    @XmlElement(name = "order_validity")            private String orderValidity;
    @XmlElement(name = "order_expiry_date")         private String orderExpiryDate;
    @XmlElement(name = "ccy")                       private String ccy;
    @XmlElement(name = "last_release_condition")    private String lastReleaseCondition;
    @XmlElement(name = "product_group")             private String productGroup;
    @XmlElement(name = "product_group_names")       private ProductGroupNames productGroupNames;
    @XmlElement(name = "contract_month")            private String contractMonth;
    @XmlElement(name = "t1_session")                private String t1Session;
    @XmlElement(name = "exec_amt")                  private BigDecimal execAmt;
}
