package com.magnumresearch.aqumon.riskengine.adapter.ib.pojo;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class Commission {

    private Integer envId;
    private String extExecutionId;
    private BrokerType brokerType;
    private String brokerAccount;
    private BigDecimal commission = BigDecimal.ZERO;

    public Commission() {
    }

    public Commission(Integer envId, String extExecutionId, BrokerType brokerType, String brokerAccount, BigDecimal commission) {
        this.envId = envId;
        this.extExecutionId = extExecutionId;
        this.brokerType = brokerType;
        this.brokerAccount = brokerAccount;
        this.commission = commission;
    }

    @Override
    public String toString() {
        return "Commission{" +
                "brokerType=" + brokerType +
                ", brokerAccount='" + brokerAccount + '\'' +
                ", envId=" + envId +
                ", extExecutionId='" + extExecutionId + '\'' +
                ", commission=" + commission +
                '}';
    }
}
