package io.github.leofuso.autoconfigure.actuator.kafka.streams.conditions;

import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.condition.NoneNestedConditions;
import org.springframework.context.annotation.Condition;

/**
 * {@link Condition} that checks for the presence of multiple beans of the same type.
 */
class OnMultipleCandidate extends NoneNestedConditions {

    /**
     * Creates a new OnMultipleCandidate instance.
     */
    public OnMultipleCandidate() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    /**
     * OnSingleCandidate negation.
     */
    @ConditionalOnSingleCandidate
    static class OnSingleCandidate {}

}
