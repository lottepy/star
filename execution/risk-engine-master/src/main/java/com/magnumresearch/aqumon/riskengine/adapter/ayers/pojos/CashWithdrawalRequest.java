package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ayers.constants.MessageConstants;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.util.XMLBooleanAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.StringWriter;
import java.math.BigDecimal;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class CashWithdrawalRequest {
    @XmlAttribute(name = "type")                    private String type;
    @XmlAttribute(name = "msgnum")                  private String msgnum;
    @XmlElement(name = "client_acc_code")           private String clientAccCode;
    @XmlElement(name = "ccy")                       private String ccy;
    @XmlElement(name = "amount")                    private BigDecimal amount;

    @XmlElement(name = "fx_transfer")
    @XmlJavaTypeAdapter(XMLBooleanAdapter.class)    private Boolean fxTransfer;

    @XmlElement(name = "remark")                    private String remark;
    @XmlElement(name = "bank_code")                 private String bankCode;
    @XmlElement(name = "bank_acc")                  private String bankAcc;

    public CashWithdrawalRequest() {
    }

    public CashWithdrawalRequest(String clientAccCode, long msgID, String ccy, BigDecimal amount, String bankCode, String bankAcc) {
        this(clientAccCode, msgID, ccy, amount, false, "", bankCode, bankAcc);
    }

    public CashWithdrawalRequest(String clientAccCode, long msgID, String ccy, BigDecimal amount, boolean isFxTransfer, String bankCode, String bankAcc) {
        this(clientAccCode, msgID, ccy, amount, isFxTransfer, "", bankCode, bankAcc);
    }


    public CashWithdrawalRequest(String clientAccCode, long msgID, String ccy, BigDecimal amount, String remark, String bankCode, String bankAcc) {
        this(clientAccCode, msgID, ccy, amount, false, remark, bankCode, bankAcc);
    }

    public CashWithdrawalRequest(String clientAccCode, long msgID, String ccy, BigDecimal amount, boolean isFxTransfer, String remark, String bankCode, String bankAcc) {
        this.type = MessageConstants.MESSAGE_TYPE_CASH_OUT_REQUEST;
        this.msgnum = String.valueOf(msgID);
        this.clientAccCode = clientAccCode;
        this.ccy = ccy;
        this.amount = amount;
        this.fxTransfer = isFxTransfer;
        this.remark = remark;
        this.bankCode = bankCode;
        this.bankAcc = bankAcc;
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(CashWithdrawalRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();
    }
}
