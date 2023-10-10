package intellistream.morphstream.util;

import intellistream.morphstream.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ClassLoaderUtils {
    private static final String NOT_FOUND_ERROR = "Unable to find %s class %s";
    private static final String ILLEGAL_ACCESS_ERROR = "Unable to access %s class %s";
    private static final String INSTANTIATION_ERROR = "Unable to instantiate %s class %s";

    public static Object newInstance(String className, String name, Logger logger) {
        if (StringUtils.isBlank(className)) {
            logger.error("A {} must be provided", name);
            throw new RuntimeException("You must provide a wrapper class");
        }
        try {
            Class<?> classObject = Class.forName(className);
            return classObject.newInstance();
        } catch (ClassNotFoundException ex) {
            String error = String.format(NOT_FOUND_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (IllegalAccessException ex) {
            String error = String.format(ILLEGAL_ACCESS_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (InstantiationException ex) {
            String error = String.format(INSTANTIATION_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        }
    }

    public static Object newInstance(String className, String name, Logger logger, int arguments) {
        if (StringUtils.isBlank(className)) {
            logger.error("A {} must be provided", name);
            throw new RuntimeException("You must provide a wrapper class");
        }
        try {
            Class<?> classObject = Class.forName(className);
            final Constructor<?> constructor = classObject.getConstructor(int.class);
            return constructor.newInstance(arguments);
        } catch (ClassNotFoundException ex) {
            String error = String.format(NOT_FOUND_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (IllegalAccessException ex) {
            String error = String.format(ILLEGAL_ACCESS_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (InstantiationException ex) {
            String error = String.format(INSTANTIATION_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (NoSuchMethodException | InvocationTargetException ex) {
            String error = String.format(INSTANTIATION_ERROR, name, className);
            ex.printStackTrace();
            throw new RuntimeException(error, ex);
        }
    }

    public static Object newInstance(String className, String name, Logger logger, Configuration arguments) {
        if (StringUtils.isBlank(className)) {
            logger.error("A {} must be provided", name);
            throw new RuntimeException("You must provide a wrapper class");
        }
        try {
            Class<?> classObject = Class.forName(className);
            final Constructor<?> constructor = classObject.getConstructor(Configuration.class);
            return constructor.newInstance(arguments);
        } catch (ClassNotFoundException ex) {
            String error = String.format(NOT_FOUND_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (IllegalAccessException ex) {
            String error = String.format(ILLEGAL_ACCESS_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (InstantiationException ex) {
            String error = String.format(INSTANTIATION_ERROR, name, className);
            logger.error(error, ex);
            throw new RuntimeException(error, ex);
        } catch (NoSuchMethodException | InvocationTargetException ex) {
            String error = String.format(INSTANTIATION_ERROR, name, className);
            ex.printStackTrace();
            throw new RuntimeException(error, ex);
        }
    }
}