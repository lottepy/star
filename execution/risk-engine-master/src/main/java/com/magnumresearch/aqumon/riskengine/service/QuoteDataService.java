package com.magnumresearch.aqumon.riskengine.service;

import com.google.common.collect.Iterables;
import com.magnumresearch.aqumon.common.feign.QuoteEngineFeignClient;
import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.quoteengine.pojo.SnapshotResponse;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

@Service
public class QuoteDataService {

    @Autowired
    QuoteEngineFeignClient quoteEngineFeignClient;

    public static final int SIZE_EACH_PARTITION = 10;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Get quote objects for the given symbols list from QuoteEngine
     * @param symbols list of symbols
     * @param brokerType source of quotes, currently supporting Neutral (from data master), KUAIRUI or BOCI
     * @param enableStreaming true to get data from streaming, otherwise from snapshot
     * @return snapshots of quotes from designated broker type for the given symbols list
     */
    public Map<String, SnapshotResponse> getQuotesFromDM(BrokerType brokerType, Set<String> symbols, boolean enableStreaming) {
        Map<String, SnapshotResponse> quoteSnapshotMap = new HashMap<>();

        Iterable<List<String>> batchSymbols = Iterables.partition(symbols, SIZE_EACH_PARTITION);
        for (List<String> batch: batchSymbols) {
            Result<List<SnapshotResponse>> quoteEngineResult;
            quoteEngineResult = quoteEngineFeignClient.getSnapshot(new ArrayList<>(batch), brokerType, enableStreaming);
            if (quoteEngineResult.getStatus().getEcode() != 0)
                log.error("[RiskEngine][Quote] get snapshot data error: " + quoteEngineResult.getStatus().getMessage());

            for (SnapshotResponse quoteSnapshot : quoteEngineResult.getData()) {
                ValidationKey validationKey = new ValidationKey(RiskGroupType.QUOTE, brokerType, null, null, quoteSnapshot.instrumentId);
                String key = validationKey.toString();
                quoteSnapshotMap.put(key, quoteSnapshot);
            }
        }
        return quoteSnapshotMap;
    }

    /**
     * calculate spreads from a map of symbols and quotes
     * @param quotes a map of symbol to quote snapshot
     * @return a map of symbol to spread data
     */
    public Map<String, BigDecimal> calculateSpreads(Map<String, SnapshotResponse> quotes) {
        Map<String, BigDecimal> resultMap = new HashMap<>();
        for (Map.Entry<String, SnapshotResponse> entry: quotes.entrySet()) {
            BigDecimal spread = calculateSpread(entry.getValue());
            resultMap.put(entry.getKey(), spread);
        }
        return resultMap;
    }

    /**
     * calculate spread from a quote
     * @param quote quote snapshot
     * @return spread data
     */
    public BigDecimal calculateSpread(SnapshotResponse quote) {
        if (Objects.isNull(quote.getA1()) || Objects.isNull(quote.getB1()))
            return null;
        else
            return quote.a1.subtract(quote.b1);
    }
}
