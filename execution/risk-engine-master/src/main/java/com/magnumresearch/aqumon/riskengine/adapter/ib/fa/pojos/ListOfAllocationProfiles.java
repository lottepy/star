package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.FAInfoBase;
import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.AllocationProfile;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Created by AqumonCL on 30/11/2016.
 */
@XmlRootElement(name = "ListOfAllocationProfiles")
@XmlAccessorType(XmlAccessType.FIELD)
public class ListOfAllocationProfiles extends FAInfoBase {
    @XmlElement(name = "AllocationProfile")
    private List<AllocationProfile> allocationProfiles;

    public List<AllocationProfile> getAllocationProfiles() {
        return allocationProfiles;
    }

    public void setAllocationProfiles(List<AllocationProfile> allocationProfiles) {
        this.allocationProfiles = allocationProfiles;
    }
}
