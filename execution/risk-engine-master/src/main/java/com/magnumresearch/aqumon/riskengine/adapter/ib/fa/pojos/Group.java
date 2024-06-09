package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.ListOfAccts;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by AqumonCL on 3/1/2017.
 */
@XmlType(propOrder = {"name", "listOfAccts","defaultMethod"})
@XmlAccessorType(XmlAccessType.FIELD)
public class Group {

    private String name;

    @XmlElement(name = "ListOfAccts")
    private ListOfAccts listOfAccts;

    private String defaultMethod;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ListOfAccts getListOfAccts() {
        return listOfAccts;
    }

    public void setListOfAccts(ListOfAccts listOfAccts) {
        this.listOfAccts = listOfAccts;
    }

    public String getDefaultMethod() {
        return defaultMethod;
    }

    public void setDefaultMethod(String defaultMethod) {
        this.defaultMethod = defaultMethod;
    }
}
