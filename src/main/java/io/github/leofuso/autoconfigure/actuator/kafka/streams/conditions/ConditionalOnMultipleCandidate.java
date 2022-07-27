package io.github.leofuso.autoconfigure.actuator.kafka.streams.conditions;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.context.annotation.Conditional;

/**
 * {@link Conditional @Conditional} that only matches when multiple beans of the specified class were already contained
 * in the {@link BeanFactory} and no single candidate can be determined.
 * <p>Exactly opposite of
 * {@link org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate ConditionalOnSingleCandidate}.
 * Specially useful for grouping beans of same type together.
 * </p>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(OnMultipleCandidate.class)
public @interface ConditionalOnMultipleCandidate {

    /**
     * The class type of bean that should be checked. The condition matches if a bean of the class specified is
     * contained in the {@link BeanFactory} and a primary candidate exists if of multiple instances.
     * <p>
     * This attribute may <strong>not</strong> be used in conjunction with {@link #type()}, but it may be used instead
     * of {@link #type()}.
     *
     * @return the class type of the bean to check
     */
    Class<?> value() default Object.class;

    /**
     * The class type name of bean that should be checked. The condition matches if a bean of the class specified is
     * contained in the {@link BeanFactory} and a primary candidate exists in case of multiple instances.
     * <p>
     * This attribute may <strong>not</strong> be used in conjunction with {@link #value()}, but it may be used instead
     * of {@link #value()}.
     *
     * @return the class type name of the bean to check
     */
    String type() default "";

    /**
     * Strategy to decide if the application context hierarchy (parent contexts) should be considered.
     *
     * @return the search strategy
     */
    SearchStrategy search() default SearchStrategy.ALL;

}