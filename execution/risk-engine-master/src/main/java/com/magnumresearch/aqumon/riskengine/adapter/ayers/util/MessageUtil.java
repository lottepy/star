package com.magnumresearch.aqumon.riskengine.adapter.ayers.util;

import com.magnumresearch.aqumon.trading.constants.OrderStatusType;
import com.magnumresearch.aqumon.trading.model.Order;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

@Component
public class MessageUtil {
    private static String env;
    @Value("${spring.profiles.active}") private void setEnv(String profiles) {MessageUtil.env = profiles;}

    public static DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

    public static String getReferenceFromOrder(Order order) {
        return "AQM-" + env + "-" + order.getBrokerAccount() + "-" + order.getId();
    }

    public static OrderStatusType getAqumonOrderStatusFromAyersOrderStatus(String status, String lastOrderActionCode) {
        switch (status) {
            case "NEW":
            case "WA":
            case "PRO":
                return OrderStatusType.PENDING;
            case "PEX":
            case "Q":
                if (lastOrderActionCode.equals("Cancel")) return OrderStatusType.CANCELLED;
                return OrderStatusType.FILLING;
            case "FEX":
                return OrderStatusType.FILLED;
            case "CAN":
                return OrderStatusType.CANCELLED;
            case "REJ":
                return OrderStatusType.REJECTED;
            default:
                return OrderStatusType.REJECTED;
        }
    }

    public static String getReferenceFromOrderId(String acct, String orderId) {
        return "AQM-" + acct + "-" + orderId;
    }

    public static String getReferenceFromMsgnum(String acct, String msgnum) {
        return "AQM-" + acct + "-" + msgnum;
    }

    public static String getOrderIdFromReference(String reference) {
        String[] splitted = reference.split("-");
        return splitted[2];
    }

    public static byte[] buildBytesFromString(String message) throws UnsupportedEncodingException {
        byte[] messageBody = message.getBytes("UTF-8");
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(message.length()).array();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + messageBody.length);
        byteBuffer.put(lengthBytes[3]);
        byteBuffer.put(lengthBytes[2]);
        byteBuffer.put(lengthBytes[1]);
        byteBuffer.put(lengthBytes[0]);
        byteBuffer.put(messageBody);

        return byteBuffer.array();
    }

    public static Document parseDocumentFromXML(String xml) {
        try {
            DocumentBuilder builder = dbFactory.newDocumentBuilder();

            Document document = builder.parse(new InputSource(new StringReader(xml)));
            return document;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getMessageType(Document document) {
        return document.getDocumentElement().getAttribute("type");
    }

    public static String getMessageType(String xml) {
        Document document = parseDocumentFromXML(xml);
        return getMessageType(document);
    }
}
