package com.magnumresearch.aqumon.riskengine.adapter.ib.util;

import com.ib.client.Contract;
import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.constants.SecType;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ContractUtils {
    @Autowired
    InstrumentUtil instrumentUtil;

    // 这个接口不会抛出异常，如果遇到找不到的合约翻译，会返回属性为null的InstrumentDataMaster标识缺少数据
    public InstrumentDataMaster getInstrumentByIBContract(Contract contract) {
        InstrumentDataMaster instrumentDataMaster = new InstrumentDataMaster();
        SecType contractSecType = SecType.valueOf(contract.secType().toString());
        String instrumentSymbol = null;
        try {
            instrumentSymbol = contractSecType == SecType.FUT ? contract.localSymbol() : contract.symbol();
            String currency = contract.currency();
            if (currency.equals("CNH")) {
                // 如果是人民币
                instrumentDataMaster = instrumentUtil.getInstrumentFromDM(
                        instrumentSymbol, contract.exchange(), null, contract.secType().toString(), null
                );
            } else {
                if (currency.equals("GBP")) currency = "GBX"; // 如果是英镑
                if (contract.secType().toString().equals("STK")) {
                    // 如果IB这边是STK，有可能DM是STK或者ETF的两种情况
                    instrumentDataMaster = instrumentUtil.getInstrumentFromDM(
                            instrumentSymbol, contract.exchange(), currency, null, null
                    );
                } else {
                    instrumentDataMaster = instrumentUtil.getInstrumentFromDM(
                            instrumentSymbol, contract.exchange(), currency, contract.secType().toString(), null
                    );
                }
            }
        } catch (Exception e) {
            log.error("Cannot get the right instrument id from datamaster! contract => " + contract, e);
            instrumentDataMaster.setSymbol(instrumentSymbol);
            instrumentDataMaster.setExchange(contract.exchange());
            instrumentDataMaster.setSecType(contractSecType);
        }
        return instrumentDataMaster;
    }

    public Contract getIBContract(InstrumentDataMaster instrumentDataMaster) {
        Contract contract = new Contract();

        SecType secType = instrumentDataMaster.getSecType();
        contract.symbol(instrumentDataMaster.getSymbol());
        contract.exchange("SMART");
        contract.primaryExch(instrumentDataMaster.getExchange());
        if (secType == SecType.ETF) {
            contract.secType(SecType.STK.toString());
        } else {
            contract.secType(secType.toString());
        }
        // IB的货币处理
        CurrencyType currency = instrumentDataMaster.getCurrency();
        if (currency == CurrencyType.CNY) currency = CurrencyType.CNH;
        if (currency == CurrencyType.GBX) currency = CurrencyType.GBP;
        contract.currency(currency.getCurrency());
        if (secType == SecType.CASH) {
            //对于货币转换的contract来说需要特殊的交易所
            contract.exchange("FXCONV");
        } else if (secType == SecType.FUT) {
            //对于期货来说需要使用localSymbol
            contract.localSymbol(instrumentDataMaster.getSymbol());
            contract.symbol(null);
        }
        return contract;
    }
}
