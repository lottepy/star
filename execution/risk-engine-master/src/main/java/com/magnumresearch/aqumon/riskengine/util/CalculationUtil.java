package com.magnumresearch.aqumon.riskengine.util;

import com.magnumresearch.aqumon.riskengine.constants.CalculationType;
import com.magnumresearch.aqumon.riskengine.constants.CompareType;
import com.magnumresearch.aqumon.riskengine.constants.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

public class CalculationUtil {

    /**
     * Find and run validation method based on input data
     * @param source source data object
     * @param target target data object
     * @param threshold threshold cf differences
     * @param calcType type of calculation
     * @param compareType type of comparisons, defined in CompareType Class
     * @return true if validation passed, otherwise false
     * @throws NoSuchFieldException CompareType or DataType or Field on SnapshotResponse Class can not be found 
     */
    public static boolean runValidationMethod(Object source, Object target, Double threshold, CalculationType calcType, CompareType compareType) throws NoSuchFieldException {

        if (Objects.isNull(source) && Objects.isNull(target))
            return true;
        else if (Objects.isNull(source) || Objects.isNull(target))
            return false;

        DataType sourceType = DataType.getDataType(source.getClass().getSimpleName());
        DataType targetType = DataType.getDataType(target.getClass().getSimpleName());

        if ((compareType == CompareType.ABSOLUTE || compareType == CompareType.PERCENTAGE) && Objects.isNull(threshold))
            throw new NoSuchFieldException("[RiskEngine] threshold is null for ABSOLUTE/PERCENTAGE compareType");

        boolean isValidated = true;
        if (sourceType == DataType.BIGDECIMAL && targetType == DataType.BIGDECIMAL) {
            if (compareType == CompareType.ABSOLUTE)
                isValidated = CalculationUtil.compareBigDecimalByAbsValue((BigDecimal) source, (BigDecimal) target, new BigDecimal(threshold), calcType);
            else if (compareType == CompareType.PERCENTAGE)
                isValidated = CalculationUtil.compareBigDecimalByPrecentage((BigDecimal) source, (BigDecimal) target, new BigDecimal(threshold), calcType);
        }
        else if ((sourceType == DataType.INTEGER && targetType == DataType.INTEGER) || (sourceType == DataType.LONG && targetType == DataType.LONG)) {
            if (compareType == CompareType.ABSOLUTE)
                isValidated = CalculationUtil.compareIntegerByAbsValue(source, target, threshold, calcType);
            else if (compareType == CompareType.PERCENTAGE)
                isValidated = CalculationUtil.compareIntegerByPercentage(source, target, threshold, calcType);
        }
        else if (sourceType == DataType.STRING && targetType == DataType.STRING) {
            if (compareType == CompareType.IGNORECASE)
                isValidated = CalculationUtil.compareStringsIgnoreCase(String.valueOf(source), String.valueOf(target));
            else if (compareType == CompareType.WITHCASE)
                isValidated = CalculationUtil.compareStringsWithCase(String.valueOf(source), String.valueOf(target));
        }

        return isValidated;
    }

    /**
     * Check if two strings are the same when ignoring cases
     * @param s1 string 1
     * @param s2 string 2
     * @return true if two string are the same when ignoring case, otherwise false
     */
    public static boolean compareStringsIgnoreCase(String s1, String s2) {
        return s1.equalsIgnoreCase(s2);
    }

    /**
     * Check if two strings are the same when keeping cases
     * @param s1 string 1
     * @param s2 string 2
     * @return true if two strings are the same when keeping cases, otherwise false
     */
    public static boolean compareStringsWithCase(String s1, String s2) {
        return s1.equals(s2);
    }
    
    /**
     * Check if the percentage difference of two BigDecimals is within threshold
     * @param val1 big decimal value 1
     * @param val2 big decimal value 2
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given BigDecimals are within threshold, otherwise false
     * @throws NoSuchFieldException calculation type is not defined
     */
    public static boolean compareBigDecimalByPrecentage(BigDecimal val1, BigDecimal val2, BigDecimal threshold, CalculationType calcType) throws NoSuchFieldException {
        if (val1 == null && val2 == null)
            return true;
        else if (val1 == null || val2 == null)
            return false;

        val1 = val1.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        val2 = val2.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        BigDecimal result;
        if (val1.compareTo(BigDecimal.ZERO) != 0 && val2.compareTo(BigDecimal.ZERO) == 0)
            return false;
        else if (val1.compareTo(BigDecimal.ZERO) == 0 && val2.compareTo(BigDecimal.ZERO) == 0)
            result = BigDecimal.ZERO;
        else
            result = val1.abs().divide(val2.abs(), 5, RoundingMode.CEILING).subtract(BigDecimal.ONE);
        return compareBigDecimalByCalculationType(result, threshold, calcType);
    }

    /**
     * Check if the absolute difference of two BigDecimals is within threshold
     * @param val1 big decimal value 1
     * @param val2 big decimal value 2
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given BigDecimals are within threshold, otherwise false
     * @throws NoSuchFieldException calculation type is not defined
     */
    public static boolean compareBigDecimalByAbsValue(BigDecimal val1, BigDecimal val2, BigDecimal threshold, CalculationType calcType) throws NoSuchFieldException {
        if (val1 == null && val2 == null)
            return true;
        else if (val1 == null || val2 == null)
            return false;

        val1 = val1.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        val2 = val2.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        BigDecimal result = val1.subtract(val2).abs();
        return compareBigDecimalByCalculationType(result, threshold, calcType);
    }

    /**
     * Check if the percentage difference of two number is within threshold
     * @param t1 number 1
     * @param t2 number 2
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given number are within threshold, otherwise false
     */
    public static boolean compareIntegerByPercentage(Object t1, Object t2, Double threshold, CalculationType calcType) throws NoSuchFieldException {
        long v1 = (long) t1;
        long v2 = (long) t2;
        if (v1 == 0 && v2 == 0)
            return true;
        else if (v1 != 0 && v2 == 0)
            return false;
        long result = Math.abs(v1)/Math.abs(v2) - 1;
        return compareIntegerByCalculationType(result, threshold.longValue(), calcType);
    }

    /**
     * Check if the absolute difference of two number is within threshold
     * @param t1 number 1
     * @param t2 number 2
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given number are within threshold, otherwise false
     */
    public static boolean compareIntegerByAbsValue(Object t1, Object t2, Double threshold, CalculationType calcType) throws NoSuchFieldException {
        long v1 = (long) t1;
        long v2 = (long) t2;
        long result = v1 - v2;
        return compareIntegerByCalculationType(result, threshold.longValue(), calcType);
    }

    /**
     * compare big decimal by calculation type
     * @param result big decimal result
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given BigDecimals are within threshold, otherwise false
     * @throws NoSuchFieldException calculation type is not defined
     */
    public static boolean compareBigDecimalByCalculationType(BigDecimal result, BigDecimal threshold, CalculationType calcType) throws NoSuchFieldException {
        result = result.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        threshold = threshold.setScale(2, BigDecimal.ROUND_HALF_EVEN);
        if (calcType == CalculationType.DIFFERENCE)
            return result.abs().compareTo(threshold) <= 0;
        else if (calcType == CalculationType.EQUAL)
            return result.compareTo(threshold) == 0;
        else if (calcType == CalculationType.NOTEQUAL)
            return result.compareTo(threshold) != 0;
        else if (calcType == CalculationType.GREATERTHAN)
            return result.compareTo(threshold) > 0;
        else if (calcType == CalculationType.GREATEROREQUAL)
            return result.compareTo(threshold) >= 0;
        else if (calcType == CalculationType.LESSTHAN)
            return result.compareTo(threshold) < 0;
        else if (calcType == CalculationType.LESSOREUQAL)
            return result.compareTo(threshold) <= 0;
        else
            throw new NoSuchFieldException("[RiskEngine] calculation type '" + calcType.name() + "' is not defined in enum CalculationType");
    }

    /**
     * compare number by calculation type
     * @param result big decimal result
     * @param threshold user defined threshold
     * @param calcType type of calculation
     * @return true if the difference for two given number are within threshold, otherwise false
     * @throws NoSuchFieldException calculation type is not defined
     */
    public static boolean compareIntegerByCalculationType(Long result, Long threshold, CalculationType calcType) throws NoSuchFieldException {
        if (calcType == CalculationType.DIFFERENCE)
            return Math.abs(result) <= threshold;
        else if (calcType == CalculationType.EQUAL)
            return result.equals(threshold);
        else if (calcType == CalculationType.NOTEQUAL)
            return !result.equals(threshold);
        else if (calcType == CalculationType.GREATERTHAN)
            return result > threshold;
        else if (calcType == CalculationType.GREATEROREQUAL)
            return result >= threshold;
        else if (calcType == CalculationType.LESSTHAN)
            return result < threshold;
        else if (calcType == CalculationType.LESSOREUQAL)
            return result <= threshold;
        else
            throw new NoSuchFieldException("[RiskEngine] calculation type '" + calcType.name() + "' is not defined in enum CalculationType");
    }
}
