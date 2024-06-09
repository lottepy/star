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
public class LoginRequest {
    @XmlAttribute(name = "type")            private String type;
    @XmlAttribute(name = "msgnum")          private String msgnum;
    @XmlElement(name = "site")              private String site;
    @XmlElement(name = "station")           private String station;
    @XmlElement(name = "user")              private String user;
    @XmlElement(name = "password")          private String password;
    @XmlElement(name = "order_recovery")    private String orderRecovery;

    public LoginRequest() {
    }

    public LoginRequest(long msgID, String site, String station, String user, String password, String orderRecovery) {
        this.type = MessageConstants.MESSAGE_TYPE_LOGIN_REQUEST;
        this.msgnum = String.valueOf(msgID);
        this.site = site;
        this.station = station;
        this.user = user;
        this.password = password;
        this.orderRecovery = orderRecovery;
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(LoginRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();

    }
}
