package com.magnumresearch.aqumon.riskengine.adapter.ib.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.math.BigDecimal;

@ApiModel(value = "IB汇率")
@JsonIgnoreProperties(ignoreUnknown = true)
public class IBQuote {

    @ApiModelProperty(value = "时间戳")
    @JsonProperty("timestamp")
    private Long timestamp;

    @ApiModelProperty(value = "价格")
    @JsonProperty("price")
    private BigDecimal price;

    @ApiModelProperty(value = "iuid")
    @JsonProperty("iuid")
    private String iuid;

    @ApiModelProperty(value = "输入币种")
    @JsonProperty("type")
    private String type;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getIuid() {
        return iuid;
    }

    public void setIuid(String iuid) {
        this.iuid = iuid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "IBQuote [timestamp=" + timestamp + ", price=" + price + ", iuid=" + iuid + ", type=" + type + "]";
    }

}
