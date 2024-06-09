package com.magnumresearch.aqumon.riskengine.service;

import com.google.gson.Gson;
import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.*;
import com.magnumresearch.aqumon.riskengine.pojo.ComparisonResult;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.riskengine.util.CalculationUtil;
import com.magnumresearch.aqumon.riskengine.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;

@Service
public class BaseValidationService {

    public static final String OBJECT = "object";

    @Autowired
    LarkRobotClient larkRobotClient;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * compare map of objects between source and target
     * @param sources a map of source data
     * @param targets a map of target data
     * @param validationRules a set of validation rules
     * @param riskGroupType risk group type of comparison
     * @param originSource origin data source
     * @param targetSource target data source
     * @param defaultField default fields if field filter is not specified
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @return a map of key with different objects if values diff from source and target are beyond the threshold
     */
    public List<ValidationData> compareObjectMap(Map<String, Object> sources, Map<String, Object> targets, Set<BaseRulesConfig.ValidationRule> validationRules, RiskGroupType riskGroupType, DataSourceType originSource, DataSourceType targetSource, String defaultField, boolean includeObjects) {
        log.info("[RiskEngine][Validation][" + riskGroupType + "] validation starts");
        List<ValidationData> valueResults = new ArrayList<>();
        List<ValidationData> objectResults = new ArrayList<>();
        if (Objects.isNull(sources) || sources.size()==0)
            log.warn("[RiskEngine][Validation][" + riskGroupType + "] no source holdings found for comparison");
        if (Objects.isNull(targets) || targets.size()==0)
            log.warn("[RiskEngine][Validation][" + riskGroupType + "] no target holdings found for comparison");

        for (Map.Entry<String, Object> entry : sources.entrySet()) {
            Object source = entry.getValue();
            Object target = targets.get(entry.getKey());
            Map<String, ComparisonResult> valueResult = compareObjects(entry.getKey(), source, target, validationRules, riskGroupType, originSource, targetSource, defaultField);
            Map<String, ComparisonResult> objectResult = generateObjectPair(source, target, valueResult, includeObjects);
            if (valueResult.size() > 0) {
                ValidationData valueResultObj = new ValidationData(ValidationKey.parseKey(entry.getKey()), valueResult);
                valueResults.add(valueResultObj);
            }
            if (objectResult.size() > 0) {
                ValidationData valueResultObj = new ValidationData(ValidationKey.parseKey(entry.getKey()), objectResult);
                objectResults.add(valueResultObj);
            }
            targets.remove(entry.getKey());
        }

        for (Map.Entry<String, Object> entry: targets.entrySet()) {
            Object target = entry.getValue();
            Map<String, ComparisonResult> valueResult = compareObjects(entry.getKey(), null, target, validationRules, riskGroupType, originSource, targetSource, defaultField);
            Map<String, ComparisonResult> objectResult = generateObjectPair(null, target, valueResult, includeObjects);

            if (valueResult.size() > 0) {
                ValidationData valueResultObj = new ValidationData(ValidationKey.parseKey(entry.getKey()), valueResult);
                valueResults.add(valueResultObj);
            }
            if (objectResult.size() > 0) {
                ValidationData valueResultObj = new ValidationData(ValidationKey.parseKey(entry.getKey()), objectResult);
                objectResults.add(valueResultObj);
            }
        }

        if (valueResults.size() > 0) {
            String message = buildDingDingMessage(riskGroupType, valueResults, originSource, targetSource);
            larkRobotClient.sendMessage(message);
        }

        log.info("[RiskEngine][Validation][" + riskGroupType + "] validation completed");
        return objectResults;
    }

    /**
     * compare source and target and return a map of key and comparison results if the rule is breached
     * @param key key string
     * @param source source data
     * @param target target data
     * @param validationRules a set of validation rules
     * @param riskGroupType risk group type of comparison
     * @param originSource origin data source
     * @param targetSource target data source
     * @param defaultField default fields if field filter is not specified
     * @return a map of rule-breached values for fields that specified in config for validation in source and target
     */
    public Map<String, ComparisonResult> compareObjects(String key, Object source, Object target, Set<BaseRulesConfig.ValidationRule> validationRules, RiskGroupType riskGroupType, DataSourceType originSource, DataSourceType targetSource, String defaultField) {
        Map<String, ComparisonResult> resultMap = new HashMap<>();
        if (Objects.isNull(validationRules) || validationRules.size() == 0) {
            log.info("[RiskEngine][Validation][" + riskGroupType + "] validation passed as no config found");
            return resultMap;
        }

        for (BaseRulesConfig.ValidationRule property: validationRules) {
            Object sourceValue = source;
            Object targetValue = target;
            Double threshold = property.getThreshold();
            CalculationType calcType = property.getCalculationType();
            CompareType compareType = property.getCompareType();

            if (Objects.nonNull(property.getFieldNames()) && property.getFieldNames().size() > 0) {
                for (String field : property.getFieldNames()) {
                    try {
                        sourceValue = PropertyUtil.getPropertyField(field, source);
                        targetValue = PropertyUtil.getPropertyField(field, target);
                        ComparisonResult comparisonResult = compareFields(key, sourceValue, targetValue, field, threshold, calcType, compareType, riskGroupType, originSource, targetSource);
                        if (Objects.nonNull(comparisonResult))
                            resultMap.put(field, comparisonResult);
                    } catch (IllegalAccessException illegalAccessException) {
                        log.error("[RiskEngine][Validation][" + riskGroupType + "] validation failed due no access to class field, ignored: Key=" + key + ", FieldName=" + field + ", Exception=", illegalAccessException);
                    } catch (NoSuchFieldException noSuchFieldException) {
                        log.error("[RiskEngine][Validation][" + riskGroupType + "] validation failed due to fields/configs not found, ignored: Key=" + key + ", FieldName=" + field + ", Exception=", noSuchFieldException);
                    }
                }
            }
            else {
                try {
                    DataType.getDataType(sourceValue.getClass().getSimpleName());
                    DataType.getDataType(targetValue.getClass().getSimpleName());
                    ComparisonResult comparisonResult = compareFields(key, sourceValue, targetValue, defaultField, threshold, calcType, compareType, riskGroupType, originSource, targetSource);
                    if (Objects.nonNull(comparisonResult))
                        resultMap.put(defaultField, comparisonResult);
                } catch (NoSuchFieldException noSuchFieldException) {
                    log.error("[RiskEngine][Validation] validation failed due to due to source/target value data type is not supported, ignored: RiskGroup=" + riskGroupType.name() + ", Key=" + key + ", FieldName=" + defaultField + ", Exception=", noSuchFieldException);
                }

            }
        }
        return resultMap;
    }

    /**
     * compare field value between source and target and return the comparison results if validation rule failed
     * @param key key string
     * @param sourceValue field value from source
     * @param targetValue field value from target
     * @param field field to be compared on
     * @param threshold threshold of comparison result
     * @param calcType calculation type
     * @param compareType comparison type
     * @param riskGroupType risk group type
     * @param originSource origin data source
     * @param targetSource target data source
     * @return comparison result
     * @throws NoSuchFieldException dataType is not supported in source or target object
     */
    public ComparisonResult compareFields(String key, Object sourceValue, Object targetValue, String field, Double threshold, CalculationType calcType, CompareType compareType, RiskGroupType riskGroupType, DataSourceType originSource, DataSourceType targetSource) throws NoSuchFieldException {
        ComparisonResult compareResult = null;
        if (CalculationUtil.runValidationMethod(sourceValue, targetValue, threshold, calcType, compareType)) {
            log.info("[RiskEngine][Validation][" + riskGroupType + "] validation passed: Key=" + key + ", FieldName=" + field + ", " + originSource + "=" + sourceValue + ", " + targetSource + "=" + targetValue +
                    ", Threshold=" + threshold + ", CalculationType=" + calcType + ", CompareType=" + compareType);
        }
        else {
            log.error("[RiskEngine][Validation][" + riskGroupType + "] validation failed: Key=" + key + ", FieldName=" + field + ", " + originSource + "=" + sourceValue + ", " + targetSource + "=" + targetValue +
                    ", Threshold=" + threshold + ", CalculationType=" + calcType + ", CompareType=" + compareType);
            compareResult = new ComparisonResult(sourceValue, targetValue, threshold, compareType, calcType);
        }
        return compareResult;
    }

    /**
     * convert a map of values differences to a map of objects containing the differences
     * @param source source object
     * @param target target object
     * @param resultMap a map of key and comparison result containing only the value differences between source and target
     * @param includeObjects true to store objects instead of values differences of pre-config fields
     * @return a map of key and comparison result containing two objects if there is differences between source and target
     */
    public Map<String, ComparisonResult> generateObjectPair(Object source, Object target, Map<String, ComparisonResult> resultMap, boolean includeObjects) {
        if (resultMap.size() > 0 && includeObjects) {
            Map<String, ComparisonResult> objectResultMap = new HashMap<>();
            ComparisonResult comparisonResult = new ComparisonResult(source, target);
            objectResultMap.put(OBJECT, comparisonResult);
            return objectResultMap;
        }
        else
            return resultMap;
    }

    /**
     * construct objects map based on default values in configuration
     * @param obj input object
     * @param keys keys from source
     * @param validationRules validation rules with default values
     * @param riskGroupType risk group type
     * @return a map of key and object based on default values in configuration
     */
    public Map<String, Object> constructDefaultObjects(Object obj, Set<String> keys, Set<BaseRulesConfig.ValidationRule> validationRules, RiskGroupType riskGroupType) {
        Gson gson = new Gson();
        Map<String, Object> defaultObjects = new HashMap<>();
        Object defaultQuote = setDefaultObject(obj, validationRules, riskGroupType);
        for (String key: keys) {
            Object cloneObject = gson.fromJson(gson.toJson(defaultQuote), obj.getClass());
            defaultObjects.put(key, cloneObject);
        }
        return defaultObjects;
    }

    /**
     * construct object with default value
     * @param validationRules validation rules with default values
     * @param riskGroupType risk group type
     * @return default object with default value
     */
    public Object setDefaultObject(Object obj, Set<BaseRulesConfig.ValidationRule> validationRules, RiskGroupType riskGroupType) {
        for (BaseRulesConfig.ValidationRule validationRule: validationRules) {
            for (String fieldName: validationRule.getFieldNames()) {
                Field field;
                try {
                    field = obj.getClass().getDeclaredField(fieldName);
                    field.setAccessible(true);
                    field.set(obj, BigDecimal.valueOf(validationRule.getDefaultValue()));
                } catch (NoSuchFieldException noSuchFieldException) {
                    log.error("[RiskEngine][Validation][" + riskGroupType.name() + "] validation failed due no such config/field for default object: ObjectClass=" + obj.getClass() + ", FieldName=" + fieldName + ", Exception=", noSuchFieldException);
                } catch (IllegalAccessException illegalAccessException) {
                    log.error("[RiskEngine][Validation][" + riskGroupType.name() + "] validation failed due no access to field in default object: ObjectClass=" + obj.getClass() + ", FieldName=" + fieldName + ", Exception=", illegalAccessException);
                }
            }
        }
        return obj;
    }

    /**
     * combine a list of validation data into a single dingding message
     * @param objectResults a list of validation data for dingding alert
     * @return a string of dingding message
     */
    public String buildDingDingMessage(RiskGroupType riskGroupType, List<ValidationData> objectResults, DataSourceType origin, DataSourceType target) {
        StringBuilder message = new StringBuilder();
        message.append("[RiskEngine] Validation failed: riskGroup=").append(riskGroupType).append("\n");
        int index = 0;
        for (ValidationData objectResult: objectResults) {
            for (Map.Entry<String, ComparisonResult> fieldEntry : objectResult.getFieldData().entrySet()) {
                if (index < 10) {
                    ComparisonResult result = fieldEntry.getValue();
                    message.append("#").append(index + 1).append(": key=").append(objectResult.getValidationKey().toString()).append(", field=").append(fieldEntry.getKey()).append(", ").append("origin").append("=").append(origin).append("|").append(result.getSource()).append(", ").
                            append("target").append("=").append(target).append("|").append(result.getTarget()).append(", threshold=").append(result.getThreshold()).append(", compareType=").append(result.getCalculationType()).append("|").append(result.getCompareType()).append("\n");
                }
                index = index + 1;
            }
        }
        message.append("...\n").append("Total ").append(index).append(" breaches found");
        return message.toString();
    }
}