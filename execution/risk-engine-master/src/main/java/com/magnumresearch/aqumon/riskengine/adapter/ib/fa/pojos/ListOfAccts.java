package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by AqumonCL on 3/1/2017.
 */
@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
public class ListOfAccts {
    @XmlElement(name = "String")
    private List<String> strings;
    @XmlAttribute(name = "varName")
    private String varName;

    public List<String> getStrings() {
        return strings;
    }

    public void setStrings(List<String> strings) {
        this.strings = strings;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }
}
