package com.magnumresearch.aqumon.riskengine.model;

import com.magnumresearch.aqumon.common.model.CommonDbFields;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.quant360.api.model.oes.enu.OesOrdStatus;
import com.quant360.api.model.oes.enu.OesOrdType;
import lombok.Data;

import javax.persistence.*;
import java.math.BigDecimal;

@Entity
@Data

@Table(name = "trading_ext_execution")
public class ExtExecution extends CommonDbFields {

    @Transient
    private String buy_sell_type; //Buy of Sell

    @Basic
    @Column(name = "direction")
    private int direction; //0-->BUY, 1-->SELL

    @Column(name = "price_sent", columnDefinition = "decimal(20,8)", nullable = false)
    private BigDecimal priceSent; //original order price

    @Column(name = "price_filled", columnDefinition = "decimal(20,8)", nullable = false)
    private BigDecimal priceFilled = BigDecimal.ZERO; //Price

    @Column(name = "quantity_filled", columnDefinition = "decimal(20,8)", nullable = false)
    private BigDecimal quantityFilled; //Filled quantity

    @Column(name = "amount_filled", columnDefinition = "decimal(20,8)", nullable = false)
    private BigDecimal amountFilled = BigDecimal.ZERO;

    @Column(name = "quantity_sent", columnDefinition = "decimal(20,8)", nullable = false)
    private BigDecimal quantitySent = BigDecimal.ZERO; //Total quantity

    @Column(name = "instrument_id")
    private String instrumentId;

    @Column(name = "trade_time")
    private int trade_time; //trade time

    @Column(name = "ext_order_id")
    private String ext_order_id; //ext_order_id

    @Column(name = "ext_execution_id")
    private String ext_execution_id; //ext_trade_num

    @Transient
    private OesOrdStatus order_status; //status

    @Column(name = "cl_env_id")
    private int cl_env_id; //environment id

    @Column(name = "broker_account")
    private String broker_account; //brokeraccount value, i.e., 0188800385 or A188800385

//    @Column(name = "broker_id")
//    private int broker_id;
    @Column(name = "broker_type")
    @Enumerated(EnumType.STRING)
    private BrokerType brokerType;

    @Column(name = "sub_account_id")
    private Long subAccountId;

    @Column(name = "date")
    private String date; //In the format of yyyy-mm-dd

    @Transient
    private OesOrdType oes_ord_type; //oes order type

    //return the id_list, consisted of environment id, sequence id and trade id
    public String getEOTId() {
        return this.cl_env_id + "_" + this.ext_order_id + "_" + this.ext_execution_id;
    }


}

