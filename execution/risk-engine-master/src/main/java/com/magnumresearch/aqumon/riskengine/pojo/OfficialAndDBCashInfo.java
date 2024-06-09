package com.magnumresearch.aqumon.riskengine.pojo;

import java.math.BigDecimal;

public class OfficialAndDBCashInfo {
    private Boolean matched;
    private BigDecimal official_balance;
    private BigDecimal db_sub_account_total_balance;
    private BigDecimal official_freeze_balance;
    private BigDecimal db_sub_account_total_freeze_balance;
    private BigDecimal diff_of_balance;
    private BigDecimal diff_of_freeze_balance;
    private BigDecimal allowed_difference;
    private String broker_account;

    public OfficialAndDBCashInfo()
    {
        official_balance = BigDecimal.ZERO;
        db_sub_account_total_balance = BigDecimal.ZERO;
        official_freeze_balance = BigDecimal.ZERO;
        db_sub_account_total_freeze_balance = BigDecimal.ZERO;
    }

    public BigDecimal getOfficial_balance() {
        return official_balance;
    }

    public void setOfficial_balance(BigDecimal official_balance) {
        this.official_balance = official_balance;
    }

    public BigDecimal getDb_sub_account_total_balance() {
        return db_sub_account_total_balance;
    }

    public void setDb_sub_account_total_balance(BigDecimal db_sub_account_total_balance) {
        this.db_sub_account_total_balance = db_sub_account_total_balance;
    }

    public BigDecimal getOfficial_freeze_balance() {
        return official_freeze_balance;
    }

    public void setOfficial_freeze_balance(BigDecimal official_freeze_balance) {
        this.official_freeze_balance = official_freeze_balance;
    }

    public BigDecimal getDb_sub_account_total_freeze_balance() {
        return db_sub_account_total_freeze_balance;
    }

    public void setDb_sub_account_total_freeze_balance(BigDecimal db_sub_account_total_freeze_balance) {
        this.db_sub_account_total_freeze_balance = db_sub_account_total_freeze_balance;
    }

    public BigDecimal getDiff_of_balance() {
        return diff_of_balance;
    }

    public void setDiff_of_balance(BigDecimal diff_of_balance) {
        this.diff_of_balance = diff_of_balance;
    }

    public BigDecimal getDiff_of_freeze_balance() {
        return diff_of_freeze_balance;
    }

    public void setDiff_of_freeze_balance(BigDecimal diff_of_freeze_balance) {
        this.diff_of_freeze_balance = diff_of_freeze_balance;
    }

    public Boolean getMatched() {
        return matched;
    }

    public void setMatched(Boolean matched) {
        this.matched = matched;
    }

    public BigDecimal getAllowed_difference() {
        return allowed_difference;
    }

    public void setAllowed_difference(BigDecimal allowed_difference) {
        this.allowed_difference = allowed_difference;
    }

    public String getBroker_account() {
        return broker_account;
    }

    public void setBroker_account(String broker_account) {
        this.broker_account = broker_account;
    }
}
