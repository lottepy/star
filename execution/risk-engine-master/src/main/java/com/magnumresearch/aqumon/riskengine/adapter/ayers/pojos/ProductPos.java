package com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class ProductPos {
    @XmlElement(name = "product") private List<ProductPosition> product;
}
