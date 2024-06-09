package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.util.List;

@Data
public class BOCIFilledDetail {
    List<BOCIExecution> executions;
}
