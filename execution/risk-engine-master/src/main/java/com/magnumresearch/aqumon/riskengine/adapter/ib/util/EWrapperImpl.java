package com.magnumresearch.aqumon.riskengine.adapter.ib.util;

import com.ib.client.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class EWrapperImpl implements EWrapper {

    private BlockingQueue receivingQueue;

    public EWrapperImpl(BlockingQueue receivingQueue) {
        this.receivingQueue = receivingQueue;
    }

    @Override
    public void tickPrice(int var1, int var2, double var3, TickAttrib var5){

    };

    @Override
    public void tickSize(int i, int i1, int i2) {

    }

    @Override
    public void tickOptionComputation(int i, int i1, double v, double v1, double v2, double v3, double v4, double v5, double v6, double v7) {

    }

    @Override
    public void tickGeneric(int i, int i1, double v) {

    }

    @Override
    public void tickString(int i, int i1, String s) {

    }

    @Override
    public void tickEFP(int i, int i1, double v, String s, double v1, int i2, String s1, double v2, double v3) {

    }

    @Override
    public void orderStatus(int var1, String var2, double var3, double var5, double var7, int var9, int var10, double var11, int var13, String var14, double var15) {

    }

    @Override
    public void openOrder(int i, Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void openOrderEnd() {

    }

    @Override
    public void updateAccountValue(String s, String s1, String s2, String s3) {

    }

    @Override
    public void updatePortfolio(Contract contract, double v, double v1, double v2, double v3, double v4, double v5, String s) {

    }

    @Override
    public void updateAccountTime(String s) {

    }

    @Override
    public void accountDownloadEnd(String s) {

    }

    @Override
    public void nextValidId(int i) {

    }

    @Override
    public void contractDetails(int i, ContractDetails contractDetails) {

    }

    @Override
    public void bondContractDetails(int i, ContractDetails contractDetails) {

    }

    @Override
    public void contractDetailsEnd(int i) {

    }

    @Override
    public void execDetails(int i, Contract contract, Execution execution) {

    }

    @Override
    public void execDetailsEnd(int i) {

    }

    @Override
    public void updateMktDepth(int i, int i1, int i2, int i3, double v, int i4) {

    }

    @Override
    public void updateMktDepthL2(int var1, int var2, String var3, int var4, int var5, double var6, int var8, boolean var9) {

    }

    @Override
    public void updateNewsBulletin(int i, int i1, String s, String s1) {

    }

    @Override
    public void managedAccounts(String s) {

    }

    @Override
    public void receiveFA(int i, String s) {

    }

    @Override
    public void historicalData(int var1, Bar var2) {

    }

    @Override
    public void scannerParameters(String s) {

    }

    @Override
    public void scannerData(int i, int i1, ContractDetails contractDetails, String s, String s1, String s2, String s3) {

    }

    @Override
    public void scannerDataEnd(int i) {

    }

    @Override
    public void realtimeBar(int i, long l, double v, double v1, double v2, double v3, long l1, double v4, int i1) {

    }

    @Override
    public void currentTime(long l) {

    }

    @Override
    public void fundamentalData(int i, String s) {

    }

    @Override
    public void deltaNeutralValidation(int i, DeltaNeutralContract deltaNeutralContract) {

    }

    @Override
    public void tickSnapshotEnd(int i) {

    }

    @Override
    public void marketDataType(int i, int i1) {

    }

    @Override
    public void commissionReport(CommissionReport commissionReport) {

    }

    @Override
    public void position(String s, Contract contract, double v, double v1) {

    }

    @Override
    public void positionEnd() {

    }

    @Override
    public void accountSummary(int i, String s, String s1, String s2, String s3) {

    }

    @Override
    public void accountSummaryEnd(int i) {

    }

    @Override
    public void verifyMessageAPI(String s) {

    }

    @Override
    public void verifyCompleted(boolean b, String s) {

    }

    @Override
    public void verifyAndAuthMessageAPI(String s, String s1) {

    }

    @Override
    public void verifyAndAuthCompleted(boolean b, String s) {

    }

    @Override
    public void displayGroupList(int i, String s) {

    }

    @Override
    public void displayGroupUpdated(int i, String s) {

    }

    @Override
    public void error(Exception e) {

    }

    @Override
    public void error(String s) {

    }

    @Override
    public void error(int i, int i1, String s) {

    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void connectAck() {

    }

    @Override
    public void positionMulti(int i, String s, String s1, Contract contract, double v, double v1) {

    }

    @Override
    public void positionMultiEnd(int i) {

    }

    @Override
    public void accountUpdateMulti(int i, String s, String s1, String s2, String s3, String s4) {

    }

    @Override
    public void accountUpdateMultiEnd(int i) {

    }

    @Override
    public void securityDefinitionOptionalParameter(int i, String s, int i1, String s1, String s2, Set<String> set, Set<Double> set1) {

    }

    @Override
    public void securityDefinitionOptionalParameterEnd(int i) {

    }

    @Override
    public void softDollarTiers(int i, SoftDollarTier[] softDollarTiers) {

    }

    @Override
    public void familyCodes(FamilyCode[] var1){}

    @Override
    public void symbolSamples(int var1, ContractDescription[] var2){}

    @Override
    public void historicalDataEnd(int var1, String var2, String var3){}

    @Override
    public void mktDepthExchanges(DepthMktDataDescription[] var1){}

    @Override
    public void tickNews(int var1, long var2, String var4, String var5, String var6, String var7){}

    @Override
    public void smartComponents(int var1, Map<Integer, Map.Entry<String, Character>> var2){}

    @Override
    public void tickReqParams(int var1, double var2, String var4, int var5){}

    @Override
    public void newsProviders(NewsProvider[] var1){}

    @Override
    public void newsArticle(int var1, int var2, String var3){}

    @Override
    public void historicalNews(int var1, String var2, String var3, String var4, String var5){}

    @Override
    public void historicalNewsEnd(int var1, boolean var2){}

    @Override
    public void headTimestamp(int var1, String var2){}

    @Override
    public void histogramData(int var1, List<HistogramEntry> var2){}

    @Override
    public void historicalDataUpdate(int var1, Bar var2){}

    @Override
    public void rerouteMktDataReq(int var1, int var2, String var3){}

    @Override
    public void rerouteMktDepthReq(int var1, int var2, String var3){}

    @Override
    public void marketRule(int var1, PriceIncrement[] var2){}

    @Override
    public void pnl(int var1, double var2, double var4, double var6){}

    @Override
    public void pnlSingle(int var1, int var2, double var3, double var5, double var7, double var9){}

    @Override
    public void historicalTicks(int var1, List<HistoricalTick> var2, boolean var3){}

    @Override
    public void historicalTicksBidAsk(int var1, List<HistoricalTickBidAsk> var2, boolean var3){}

    @Override
    public void historicalTicksLast(int var1, List<HistoricalTickLast> var2, boolean var3){}

    @Override
    public void tickByTickAllLast(int var1, int var2, long var3, double var5, int var7, TickAttribLast var8, String var9, String var10){}

    @Override
    public void tickByTickBidAsk(int var1, long var2, double var4, double var6, int var8, int var9, TickAttribBidAsk var10){}

    @Override
    public void tickByTickMidPoint(int var1, long var2, double var4){}

    @Override
    public void orderBound(long var1, int var3, int var4){}

    @Override
    public void completedOrder(Contract var1, Order var2, OrderState var3){}

    @Override
    public void completedOrdersEnd(){}
}
