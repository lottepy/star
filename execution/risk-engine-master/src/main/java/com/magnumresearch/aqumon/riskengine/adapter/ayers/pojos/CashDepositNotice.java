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
public class CashDepositNotice {
    @XmlAttribute(name = "type")                    private String type;
    @XmlAttribute(name = "msgnum")                  private String msgnum;
    @XmlElement(name = "client_acc_code")           private String clientAccCode;
    @XmlElement(name = "ccy")                       private String ccy;
    @XmlElement(name = "amount")                    private BigDecimal amount;

    @XmlElement(name = "fx_transfer")
    @XmlJavaTypeAdapter(XMLBooleanAdapter.class)    private Boolean fxTransfer;

    @XmlElement(name = "remark")                    private String remark;


    public CashDepositNotice() {
    }

    public CashDepositNotice(String clientAccCode, long msgID, String ccy, BigDecimal amount) {
        this(clientAccCode, msgID, ccy, amount, false, "");
    }

    public CashDepositNotice(String clientAccCode, long msgID, String ccy, BigDecimal amount, boolean isFxTransfer) {
        this(clientAccCode, msgID, ccy, amount, isFxTransfer, "");
    }


    public CashDepositNotice(String clientAccCode, long msgID, String ccy, BigDecimal amount, String remark) {
        this(clientAccCode, msgID, ccy, amount, false, remark);
    }

    public CashDepositNotice(String clientAccCode, long msgID, String ccy, BigDecimal amount, boolean isFxTransfer, String remark) {
        this.type = MessageConstants.MESSAGE_TYPE_CASH_IN_NOTICE;
        this.msgnum = String.valueOf(msgID);
        this.clientAccCode = clientAccCode;
        this.ccy = ccy;
        this.amount = amount;
        this.fxTransfer = isFxTransfer;
        this.remark = remark;
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(CashDepositNotice.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();
    }
}
