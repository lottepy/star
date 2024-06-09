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
public class OrderEnquiryRequest {
    @XmlAttribute(name = "type")    private String type;
    @XmlAttribute(name = "msgnum")  private String msgnum;
    @XmlElement(name = "reference") private String reference;

    public OrderEnquiryRequest() {
        this.type = MessageConstants.MESSAGE_TYPE_ORDER_REQUEST;
    }

    public OrderEnquiryRequest(long msgnum, String reference) {
        this.type = MessageConstants.MESSAGE_TYPE_ORDER_REQUEST;
        this.msgnum = String.valueOf(msgnum);
        this.reference = reference;
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(OrderEnquiryRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();
    }

}
