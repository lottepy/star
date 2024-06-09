package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.AyersBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.boci.BOCIBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ib.IbBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.KuanruiBrokerAdapter;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Service
public class BrokerAdapterService {

    @Autowired(required = false)
    IbBrokerAdapter ibBrokerAdapter;
    @Autowired(required = false)
    AyersBrokerAdapter ayersBrokerAdapter;
    @Autowired(required = false)
    KuanruiBrokerAdapter kuanruiBrokerAdapter;
    @Autowired(required = false)
    BOCIBrokerAdapter bociBrokerAdapter;

    private Map<BrokerType, BaseBrokerAdapter> brokerAdapterMap;

    @PostConstruct
    public void init() {
        brokerAdapterMap = new HashMap<>();
        if (ibBrokerAdapter != null) {
            brokerAdapterMap.put(BrokerType.IB, ibBrokerAdapter);
        }
        if (ayersBrokerAdapter != null) {
            brokerAdapterMap.put(BrokerType.AYERS, ayersBrokerAdapter);
        }
        if (kuanruiBrokerAdapter != null) {
            brokerAdapterMap.put(BrokerType.KR, kuanruiBrokerAdapter);
        }
        if (bociBrokerAdapter != null) {
            brokerAdapterMap.put(BrokerType.MOCKBROKER, bociBrokerAdapter);
        }
    }

    /**
     * Get adapter to broker based on broker type
     * @param brokerType broker type
     * @return base broker adapter
     * @throws TradingException no broker type found
     */
    public BaseBrokerAdapter getAdapter(BrokerType brokerType) throws TradingException {
        if (brokerAdapterMap.containsKey(brokerType)) {
            return brokerAdapterMap.get(brokerType);
        } else {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_BROKER_ADAPTER_NOT_FOUND_ERROR, "BrokerType=" + brokerType);
        }
    }


}
