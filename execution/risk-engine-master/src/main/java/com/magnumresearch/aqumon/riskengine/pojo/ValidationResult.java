package com.magnumresearch.aqumon.riskengine.pojo;

import lombok.Data;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class ValidationResult {
    private String key;
    private Date datetime;
    private List<ValidationData> results;
    private Set<String> accounts;
    private boolean isValidated;

    public ValidationResult(String key, List<ValidationData> results) {
        this.key = key;
        this.results = results;
        this.datetime = new Date();
        this.isValidated = results.size() <= 0;
        Set<String> accounts = new HashSet<>();
        for (ValidationData validationData: results)
            accounts.add(validationData.getValidationKey().getBrokerAccount());
        this.accounts = accounts;
    }
}
