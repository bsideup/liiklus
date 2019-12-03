package com.github.bsideup.liiklus.support;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.LocalDate;
import java.util.Optional;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;


@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(DisabledUntil.DisabledCondition.class)
public @interface DisabledUntil {
    /**
     * Local date, ISO formatted string such as 2000-01-30
     */
    String value();

    String comment();

    class DisabledCondition implements ExecutionCondition {

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            Optional<DisabledUntil> until = findAnnotation(context.getElement(), DisabledUntil.class);
            if (until.map(it -> LocalDate.parse(it.value()).isAfter(LocalDate.now())).orElse(false)) {
                String reason = until.map(DisabledUntil::comment).orElse("Disabled for now");
                return ConditionEvaluationResult.disabled(reason);
            }

            return ConditionEvaluationResult.enabled("Enabled");
        }
    }
}