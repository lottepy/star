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
public class ProductGroupNames {
    @XmlElement(name = "eng")   private String eng;
    @XmlElement(name = "big5")  private String big5;
    @XmlElement(name = "gb")    private String gb;
}
