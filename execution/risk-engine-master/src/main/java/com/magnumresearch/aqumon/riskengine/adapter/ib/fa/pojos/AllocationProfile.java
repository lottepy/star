package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by AqumonCL on 30/11/2016.
 */
@XmlType(propOrder = {"name", "type", "listOfAllocations"})
@XmlAccessorType(XmlAccessType.FIELD)
public class AllocationProfile {
    private String name;
    private int type;
    @XmlElement(name = "ListOfAllocations")
    private ListOfAllocations listOfAllocations;

    public ListOfAllocations getListOfAllocations() {
        return listOfAllocations;
    }

    public void setListOfAllocations(ListOfAllocations listOfAllocations) {
        this.listOfAllocations = listOfAllocations;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

}
