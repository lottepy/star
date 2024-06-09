package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class Limit {
    @XmlElement(name = "limit_code")  private String limitCode;
    @XmlElement(name = "limit_value") private String limitValue;
}
