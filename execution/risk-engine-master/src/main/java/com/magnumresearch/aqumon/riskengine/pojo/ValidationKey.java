package com.magnumresearch.aqumon.riskengine.pojo;

import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class ValidationKey {
    private RiskGroupType riskGroupType;
    private BrokerType brokerType;
    private String brokerAccount;
    private Long subAccountId;
    private String symbol;
    static Logger log = LoggerFactory.getLogger(ValidationKey.class);

    public ValidationKey() {
    }

    public ValidationKey(RiskGroupType riskGroupType, BrokerType brokerType, String brokerAccount, Long subAccountId, String symbol) {
        this.riskGroupType = riskGroupType;
        this.brokerType = brokerType;
        this.brokerAccount = brokerAccount;
        this.subAccountId = subAccountId;
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return this.riskGroupType + "_" + this.brokerType + "_" + this.brokerAccount + "_" + this.subAccountId + "_" + this.symbol;
    }

    public static ValidationKey parseKey(String key) {
        String[] data = key.split("_");
        if (data.length != 5) {
            log.info("[RiskEngine][ValidationKey] validation key failed to parse as key has less than 5 parts: key=" + key);
            return null;
        }
        try {
            RiskGroupType riskGroupType = data[0].trim().equalsIgnoreCase("null") || data[0].trim().equalsIgnoreCase("") ? null : RiskGroupType.valueOf(data[0]);
            BrokerType brokerType = data[1].trim().equalsIgnoreCase("null") || data[1].trim().equalsIgnoreCase("") ? null : BrokerType.valueOf(data[1]);
            String brokerAccount = data[2].trim().equalsIgnoreCase("null") || data[2].trim().equalsIgnoreCase("") ? null : data[2];
            Long subAccountId = data[3].trim().equalsIgnoreCase("null") || data[3].trim().equalsIgnoreCase("") ? null : Long.parseLong(data[3]);
            String symbol = data[4].trim().equalsIgnoreCase("null") || data[4].trim().equalsIgnoreCase("") ? null : data[4];
            return new ValidationKey(riskGroupType, brokerType, brokerAccount, subAccountId, symbol);
        } catch (NumberFormatException numberFormatException) {
            log.info("[RiskEngine][ValidationKey] validation key failed to parse as RiskGroupType/BrokerType can't be recognized: key=" + key + ", riskGroupType=" + data[0] + ", brokerType=" + data[1]);
            return null;
        } catch (IllegalArgumentException illegalArgumentException) {
            log.info("[RiskEngine][ValidationKey] validation key failed to parse as subAccountId can't be recognized: key=" + key + ", subAccountId=" + data[3]);
            return null;
        }
    }
}
