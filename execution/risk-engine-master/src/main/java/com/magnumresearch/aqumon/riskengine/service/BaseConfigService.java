package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.*;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class BaseConfigService {

    @Autowired
    BaseRulesConfig baseRulesConfig;

    /**
     * get a set of validation rules per risk group type
     * @param ruleName rule name
     * @param riskGroupType risk group type for rules
     * @return a set of validation rules per risk group type
     */
    public Set<BaseRulesConfig.ValidationRule> getRulesFromName(String ruleName, RiskGroupType riskGroupType) {
        Set<BaseRulesConfig.ValidationRule> ruleSet = new HashSet<>();
        Map<String, Set<BaseRulesConfig.ValidationRule>> validationRules = baseRulesConfig.parseValidationRuleJson(baseRulesConfig.getRules());
        if (Objects.isNull(ruleName) || ruleName.trim().equals("")) {
            for (Map.Entry<String, Set<BaseRulesConfig.ValidationRule>> entry : validationRules.entrySet())
                for (BaseRulesConfig.ValidationRule validationRule : entry.getValue())
                    if (validationRule.getRiskGroup() == riskGroupType)
                        ruleSet.add(validationRule);
        }
        if (validationRules.containsKey(ruleName))
            ruleSet.addAll(validationRules.get(ruleName));
        return ruleSet;
    }
}
