package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by AqumonCL on 30/11/2016.
 */
@XmlType(propOrder = {"acct", "amount"})
@XmlAccessorType(XmlAccessType.FIELD)
public class Allocation {
    private String acct;
    private String amount;

    public String getAcct() {
        return acct;
    }

    public void setAcct(String acct) {
        this.acct = acct;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }
}
