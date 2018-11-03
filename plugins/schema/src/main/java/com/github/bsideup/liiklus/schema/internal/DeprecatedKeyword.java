package com.github.bsideup.liiklus.schema.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.*;
import lombok.Getter;

import java.text.MessageFormat;
import java.util.Set;

public class DeprecatedKeyword extends AbstractKeyword {

    public DeprecatedKeyword() {
        super("deprecated");
    }

    @Override
    public JsonValidator newValidator(String schemaPath, JsonNode schemaNode, JsonSchema parentSchema, ValidationContext validationContext) {
        return new AbstractJsonValidator(getValue()) {
            @Override
            public Set<ValidationMessage> validate(JsonNode node, JsonNode rootNode, String at) {
                if (schemaNode.asBoolean()) {
                    var message = new ErrorMessageType() {

                        @Getter
                        String errorCode = "deprecated";

                        @Getter
                        MessageFormat messageFormat = new MessageFormat("{0}: is deprecated");
                    };

                    return fail(message, at);
                } else {
                    return pass();
                }
            }
        };
    }
}
