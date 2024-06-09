package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ayers.constants.MessageConstants;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class KeepAliveRequest {
    @XmlAttribute(name = "type")    private String type;
    @XmlAttribute(name = "msgnum")  private String msgnum;

    public KeepAliveRequest(long msgID) {
        this.type = MessageConstants.MESSAGE_TYPE_KEEP_ALIVE;
        this.msgnum = String.valueOf(msgID);
    }

    public KeepAliveRequest() {
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(KeepAliveRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();

    }
}