package com.magnumresearch.aqumon.riskengine.util;

import java.lang.reflect.Field;
import java.util.Objects;

public class PropertyUtil {

    /**
     * Get field object based on the input field name and object
     * @param fieldName field name of object class
     * @param object input object
     * @return field object
     * @throws NoSuchFieldException fieldName is not found on object class
     * @throws IllegalAccessException fieldName is not accessible in class
     */
    public static Object getPropertyField(String fieldName, Object object) throws NoSuchFieldException, IllegalAccessException {
        if (Objects.isNull(object))
            return null;
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(object);
    }
}
