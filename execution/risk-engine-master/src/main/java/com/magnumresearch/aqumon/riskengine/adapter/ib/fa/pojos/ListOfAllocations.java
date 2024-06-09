package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.Allocation;
import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by AqumonCL on 30/11/2016.
 */
@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
public class ListOfAllocations {
    @XmlElement(name = "Allocation")
    private List<Allocation> allocations;
    @XmlAttribute(name = "varName")
    private String varName;

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public List<Allocation> getAllocations() {
        return allocations;
    }

    public void setAllocations(List<Allocation> allocations) {
        this.allocations = allocations;
    }
}
