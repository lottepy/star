package com.magnumresearch.aqumon.riskengine.adapter.boci;

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.pojo.BOCIResult;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.boci.feign.BOCIFeignClient;
import com.magnumresearch.aqumon.riskengine.adapter.boci.pojo.BOCICashHolding;
import com.magnumresearch.aqumon.riskengine.adapter.boci.pojo.BOCIHoldingDetail;
import com.magnumresearch.aqumon.riskengine.adapter.boci.pojo.BOCIPortfolio;
import com.magnumresearch.aqumon.trading.constants.*;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.*;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "adapter.boci", value = {"enabled"})
public class BOCIBrokerAdapter extends BaseBrokerAdapter {
    @Autowired
    BOCIFeignClient bociFeignClient;
    @Autowired
    InstrumentUtil instrumentUtil;

    @Override
    public BrokerType getBrokerType() {
        return BrokerType.MOCKBROKER;
    }@SuppressWarnings("InfiniteLoopStatement")

    @Override
    public SubAccount querySubAccountSummary(SubAccount subAccount) throws TradingException {
        SubAccountSummary subAccountSummary = subAccount.getSubAccountSummary();
        BOCIResult<BOCIPortfolio> bociResult;
        String brokerAccount = subAccount.getAccount().getBrokerAccount();
        try {
            bociResult = bociFeignClient.enquirePortfolio(brokerAccount);
            log.info("[BOCI Adapter][querySubAccountSummary] BOCIResult => " + bociResult);
        } catch (FeignException e) {
            String msg = "[BOCI Adapter][querySubAccountSummary] Feign Exception, brokerAccount=" + brokerAccount;
            log.error(msg + "\n" + e);
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_BOCI_ERROR, msg, e);
        }
        if (bociResult.getData().getAccountSummary() != null) {
            String currencyCode = bociResult.getData().getAccountSummary().getCurrencyCode();
            subAccountSummary.setBaseCurrency(CurrencyType.valueOf(currencyCode));
            BigDecimal balance = bociResult.getData().getAccountSummary().getMarketValue();
            subAccountSummary.setCash(balance);
        }
        Map<CurrencyType, CashInfo> cashInfoMap = new HashMap<>();
        List<BOCICashHolding> cashHoldings = bociResult.getData().getCashHoldings();
        for (BOCICashHolding cashHolding : cashHoldings) {
            CurrencyType currencyType = CurrencyType.valueOf(cashHolding.getCurrencyCode());
            CashInfo cashInfo = new CashInfo();
            cashInfo.setBrokerAccount(brokerAccount);
            cashInfo.setBalance(cashHolding.getMarketValue());
            cashInfo.setCurrencyType(currencyType);
            cashInfoMap.put(currencyType, cashInfo);
        }
        subAccountSummary.setCashInfoMap(cashInfoMap);
        subAccount.setSubAccountSummary(subAccountSummary);
        return subAccount;
    }

    /**
     * 1. 如果 feign client 状态不是 200，抛 Exception
     * 2. 如果 没有"holdingDetails"字段则认为是没有持仓，返回空 List
     * 3. 如果 有"holdingDetails"字段则正常 loop 它一个一个转，此时也可能是没有持仓，那也是空 List
     *
     * @param brokerAccountId BOCI 的 accountId
     */
    @Override
    public List<Holding> queryHolding(String brokerAccountId) throws TradingException {
        List<Holding> holdings = new ArrayList<>();
        BOCIResult<BOCIPortfolio> bociResult;
        try {
            bociResult = bociFeignClient.enquirePortfolio(brokerAccountId);
            log.info("[BOCI Adapter][queryHolding] BOCIResult => " + bociResult);
        } catch (FeignException e) {
            String msg = "[BOCI Adapter][queryHolding] ERROR! brokerAccountId(" + brokerAccountId + ") " +
                    "Feign Exception: " + e.getMessage();
            log.error(msg + "\n" + e);
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_BOCI_ERROR, msg, e);
        }

        if (bociResult.getData() == null) return new ArrayList<>();
        BOCIPortfolio portfolio = bociResult.getData();
        if (portfolio.getHoldingDetails() == null) return new ArrayList<>();

        for (BOCIHoldingDetail holdingDetail : portfolio.getHoldingDetails()) {
            try {
                log.info("[BOCI Adapter][queryHolding][" + brokerAccountId + "] " + holdingDetail);
                if (holdingDetail.getQuantity().compareTo(BigDecimal.ZERO) != 0) {
                    Holding holding = new Holding();
                    holding.setHoldingPrice(holdingDetail.getAveragePrice());
                    holding.setHoldingPosition(holdingDetail.getQuantity());
                    InstrumentDataMaster instrumentDataMaster = instrumentUtil.getInstrumentFromDM(
                            holdingDetail.getSymbol(), null, null, null, null
                    );
                    holding.setInstrumentId(instrumentDataMaster.getInstrumentId());
                    holding.setInstrumentInfo(instrumentUtil.getInstrumentKey(instrumentDataMaster));
                    holding.setBrokerAccount(brokerAccountId);
                    holding.setSecType(SecType.ETF);
                    holding.setBrokerType(BrokerType.MOCKBROKER);
                    holdings.add(holding);
                }
            } catch (Exception e) {
                log.error("[BOCI Adapter][queryHolding] Can not add holding to holdingList, " +
                        "Maybe can not get the instrument from quote engine! " + holdingDetail);
            }
        }
        return holdings;
    }
}
