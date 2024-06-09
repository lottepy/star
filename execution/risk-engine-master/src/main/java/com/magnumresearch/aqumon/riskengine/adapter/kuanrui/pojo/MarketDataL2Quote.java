package com.magnumresearch.aqumon.riskengine.adapter.kuanrui.pojo;

import com.quant360.api.model.mds.enu.MdsSecurityType;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class MarketDataL2Quote {

    public static AtomicLong quoteIdGenerator = new AtomicLong(0);
    private long quoteId;
    private long timestamp;
    private long systimestamp;
    private String instrumentId;
    private MdsSecurityType mdsSecurityType;
    private BigDecimal last;
    private BigDecimal vol;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal b1;
    private BigDecimal bv1;
    private BigDecimal b2;
    private BigDecimal bv2;
    private BigDecimal b3;
    private BigDecimal bv3;
    private BigDecimal b4;
    private BigDecimal bv4;
    private BigDecimal b5;
    private BigDecimal bv5;
    private BigDecimal a1;
    private BigDecimal av1;
    private BigDecimal a2;
    private BigDecimal av2;
    private BigDecimal a3;
    private BigDecimal av3;
    private BigDecimal a4;
    private BigDecimal av4;
    private BigDecimal a5;
    private BigDecimal av5;
    private String MsgType;

    public MarketDataL2Quote() {
        this.quoteId = quoteIdGenerator.incrementAndGet();
    }

    public MarketDataL2Quote(String instrumentId) {
        this.instrumentId = instrumentId;
        this.quoteId = quoteIdGenerator.incrementAndGet();
    }

    public long getQuoteId() {
        return quoteId;
    }
    public void setQuoteId(long quoteId) {
        this.quoteId = quoteId;
    }

    public long getTimeStamp() {
        return timestamp;
    }
    public void setTimeStamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSysTimeStamp() {
        return systimestamp;
    }
    public void setSysTimeStamp(long systimestamp) {
        this.systimestamp = systimestamp;
    }



    public BigDecimal getLast() { return last; }
    public void setLast(BigDecimal last) { this.last=last; }

    public BigDecimal getVol() { return vol; }
    public void setVol(BigDecimal vol) { this.vol=vol; }

    public BigDecimal getOpen() { return open; }
    public void setOpen(BigDecimal open) { this.open=open; }

    public BigDecimal getHigh() { return high; }
    public void setHigh(BigDecimal high) { this.high=high; }

    public BigDecimal getLow() { return low; }
    public void setLow(BigDecimal low) { this.low=low; }

    public BigDecimal getClose() { return close; }
    public void setClose(BigDecimal close) { this.close=close; }

    public BigDecimal getA1() { return a1; }
    public void setA1(BigDecimal a1) { this.a1=a1; }

    public BigDecimal getA2() { return a2; }
    public void setA2(BigDecimal a2) { this.a2=a2; }

    public BigDecimal getA3() { return a3; }
    public void setA3(BigDecimal a3) { this.a3=a3; }

    public BigDecimal getA4() { return a4; }
    public void setA4(BigDecimal a4) { this.a4=a4; }

    public BigDecimal getA5() { return a5; }
    public void setA5(BigDecimal a5) { this.a5=a5; }

    public BigDecimal getAv1() { return av1; }
    public void setAv1(BigDecimal av1) { this.av1=av1; }

    public BigDecimal getAv2() { return av2; }
    public void setAv2(BigDecimal av2) { this.av2=av2; }

    public BigDecimal getAv3() { return av3; }
    public void setAv3(BigDecimal av3) { this.av3=av3; }

    public BigDecimal getAv4() { return av4; }
    public void setAv4(BigDecimal av4) { this.av4=av4; }

    public BigDecimal getAv5() { return av5; }
    public void setAv5(BigDecimal av5) { this.av5=av5; }

    public BigDecimal getB1() { return b1; }
    public void setB1(BigDecimal b1) { this.b1=b1; }

    public BigDecimal getB2() { return b2; }
    public void setB2(BigDecimal b2) { this.b2=b2; }

    public BigDecimal getB3() { return b3; }
    public void setB3(BigDecimal b3) { this.b3=b3; }

    public BigDecimal getB4() { return b4; }
    public void setB4(BigDecimal b4) { this.b4=b4; }

    public BigDecimal getB5() { return b5; }
    public void setB5(BigDecimal b5) { this.b5=b5; }

    public BigDecimal getBv1() { return bv1; }
    public void setBv1(BigDecimal bv1) { this.bv1=bv1; }

    public BigDecimal getBv2() { return bv2; }
    public void setBv2(BigDecimal bv2) { this.bv2=bv2; }

    public BigDecimal getBv3() { return bv3; }
    public void setBv3(BigDecimal bv3) { this.bv3=bv3; }

    public BigDecimal getBv4() { return bv4; }
    public void setBv4(BigDecimal bv4) { this.bv4=bv4; }

    public BigDecimal getBv5() { return bv5; }
    public void setBv5(BigDecimal bv5) { this.bv5=bv5; }

    public String getMsgType() { return MsgType; }
    public void setMsgType(String MsgType) { this.MsgType=MsgType; }

    @Override
    public String toString() {
        return "MarketDataL2Quote{" +
                "quoteId=" + quoteId +
                ", timestamp=" + timestamp +
                ", systimestamp=" + systimestamp +
                ", instrumentId='" + instrumentId + '\'' +
                ", mdsSecurityType=" + mdsSecurityType +
                ", last=" + last +
                ", vol=" + vol +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", b1=" + b1 +
                ", bv1=" + bv1 +
                ", b2=" + b2 +
                ", bv2=" + bv2 +
                ", b3=" + b3 +
                ", bv3=" + bv3 +
                ", b4=" + b4 +
                ", bv4=" + bv4 +
                ", b5=" + b5 +
                ", bv5=" + bv5 +
                ", a1=" + a1 +
                ", av1=" + av1 +
                ", a2=" + a2 +
                ", av2=" + av2 +
                ", a3=" + a3 +
                ", av3=" + av3 +
                ", a4=" + a4 +
                ", av4=" + av4 +
                ", a5=" + a5 +
                ", av5=" + av5 +
                ", MsgType='" + MsgType + '\'' +
                '}';
    }
}
