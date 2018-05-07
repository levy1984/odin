package com.levy.runes.kafka;

import java.io.Serializable;

public class AccountInfo implements Serializable {
    private String custId;
    private String acctId;
    private String transAmt;
    private String acctTime;

    public String getCustId() {
        return custId;
    }

    public void setCustId(String custId) {
        this.custId = custId;
    }

    public String getAcctId() {
        return acctId;
    }

    public void setAcctId(String acctId) {
        this.acctId = acctId;
    }

    public String getTransAmt() {
        return transAmt;
    }

    public void setTransAmt(String transAmt) {
        this.transAmt = transAmt;
    }

    public String getAcctTime() {
        return acctTime;
    }

    public void setAcctTime(String acctTime) {
        this.acctTime = acctTime;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AccountInfo{");
        sb.append("custId='").append(custId).append('\'');
        sb.append(", acctId='").append(acctId).append('\'');
        sb.append(", transAmt='").append(transAmt).append('\'');
        sb.append(", acctTime='").append(acctTime).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
