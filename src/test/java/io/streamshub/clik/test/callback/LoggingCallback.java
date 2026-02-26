package io.streamshub.clik.test.callback;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class LoggingCallback implements
        BeforeAllCallback,
        BeforeEachCallback,
        BeforeTestExecutionCallback,
        AfterTestExecutionCallback,
        AfterEachCallback,
        AfterAllCallback {

    private static final Logger LOGGER = Logger.getLogger(LoggingCallback.class);

    private String fullDisplayName(ExtensionContext context) {
        StringBuilder name = new StringBuilder();
        var root = context.getRoot();
        var parent = context.getParent();

        do {
            if (!name.isEmpty()) {
                name.insert(0, ' ');
            }
            name.insert(0, context.getDisplayName());
            context = parent.orElse(root);
            parent = context.getParent();
        } while (parent.isPresent() && !context.equals(root));

        return name.toString();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        LOGGER.infof("Before all %s", fullDisplayName(context));
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        LOGGER.infof("Before execution %s", fullDisplayName(context));
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        LOGGER.infof("Before test execution %s", fullDisplayName(context));
    }

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        LOGGER.infof("After test execution %s", fullDisplayName(context));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        LOGGER.infof("After test %s", fullDisplayName(context));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        LOGGER.infof("After all %s", fullDisplayName(context));
    }
}
