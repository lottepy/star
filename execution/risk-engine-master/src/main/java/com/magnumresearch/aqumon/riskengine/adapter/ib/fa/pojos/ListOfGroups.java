package com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos;

import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.FAInfoBase;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Created by AqumonCL on 3/1/2017.
 */
@XmlRootElement(name = "ListOfGroups")
@XmlAccessorType(XmlAccessType.FIELD)
public class ListOfGroups extends FAInfoBase {
    @XmlElement(name = "Group")
    private List<Group> groups;

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }
}
