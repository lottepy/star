package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ayers.constants.MessageConstants;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.*;
import java.io.StringWriter;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class ClientJournalOrdersRequest {
    @XmlAttribute(name = "type")            private String type;
    @XmlAttribute(name = "msgnum")          private String msgnum;
    @XmlElement(name = "client_acc_code")   private String clientAccCode;
    @XmlElement(name = "from_trade_date")   private String fromTradeDate;
    @XmlElement(name = "to_trade_date")     private String toTradeDate;

    public ClientJournalOrdersRequest(){

    }

    public ClientJournalOrdersRequest(String clientAcctCode, long msgID){
        this.type = MessageConstants.MESSAGE_TYPE_CLIENT_ORDER_REQUEST;
        this.msgnum = String.valueOf(msgID);
        this.clientAccCode = clientAcctCode;
    }


    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(ClientJournalOrdersRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();

    }
}
