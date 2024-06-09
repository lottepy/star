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
import java.math.BigDecimal;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class OrderRequest {
    @XmlAttribute(name = "type")            private String type;
    @XmlAttribute(name = "msgnum")          private String msgnum;
    @XmlElement(name = "bs_flag")           private String bsFlag;
    @XmlElement(name = "client_acc_code")   private String clientAccCode;
    @XmlElement(name = "exchange_code")     private String exchangeCode;
    @XmlElement(name = "product_code")      private String productCode;
    @XmlElement(name = "order_type")        private String orderType;
    @XmlElement(name = "price")             private BigDecimal price;
    @XmlElement(name = "qty")               private Integer qty;
    @XmlElement(name = "reference")         private String reference;
    @XmlElement(name = "ip_address")        private String ipAddress;

    public OrderRequest() {
    }

    public OrderRequest(String action, String msgnum, String reference, String ip) {
        switch (action) {
            case MessageConstants.MESSAGE_TYPE_CANCEL_ORDER:
                this.type = MessageConstants.MESSAGE_TYPE_CANCEL_ORDER;
                this.msgnum = msgnum;
                this.reference = reference;
                this.ipAddress = ip;
        }
    }

    public String toXML() throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(OrderRequest.class);
        Marshaller marshaller = jc.createMarshaller();
        StringWriter sw = new StringWriter();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(this, sw);

        return sw.toString();
    }
}
