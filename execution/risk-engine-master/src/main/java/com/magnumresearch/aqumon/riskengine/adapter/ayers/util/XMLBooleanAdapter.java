package com.magnumresearch.aqumon.riskengine.adapter.ayers.util;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class XMLBooleanAdapter extends XmlAdapter<String, Boolean>
{
    @Override
    public Boolean unmarshal(String s )
    {
        return (s == null) || (s.isEmpty()) ? true : s.equalsIgnoreCase("Y");
    }

    @Override
    public String marshal(Boolean c )
    {
        return c == null ? null : c ? "Y" : "N";
    }
}
