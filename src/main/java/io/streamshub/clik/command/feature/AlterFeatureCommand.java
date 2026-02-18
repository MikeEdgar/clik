package io.streamshub.clik.command.feature;

import java.util.Scanner;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.FeatureService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.FeatureInfo;
import picocli.CommandLine;

@CommandLine.Command(
    name = "alter",
    description = "Alter feature level (upgrade, downgrade, or disable)"
)
public class AlterFeatureCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
        index = "0",
        description = "Feature name (e.g., metadata.version)"
    )
    String featureName;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    AlterOptions alterOptions;

    static class AlterOptions {
        @CommandLine.Option(
            names = "--upgrade",
            description = "Upgrade to specified version level"
        )
        Short upgradeLevel;

        @CommandLine.Option(
            names = "--downgrade",
            description = "Downgrade to specified version level (requires confirmation)"
        )
        Short downgradeLevel;

        @CommandLine.Option(
            names = "--disable",
            description = "Disable/reset feature (requires confirmation)"
        )
        boolean disable;
    }

    @CommandLine.Option(
        names = {"-y", "--yes"},
        description = "Skip confirmation prompt"
    )
    boolean autoConfirm;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    FeatureService featureService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            // Verify feature exists
            FeatureInfo feature = featureService.describeFeature(admin, featureName);

            if (feature == null) {
                err().println("Error: Feature \"" + featureName + "\" not found.");
                err().println();
                err().println("Run 'clik feature list' to see available features.");
                return 1;
            }

            // Handle upgrade
            if (alterOptions.upgradeLevel != null) {
                return handleUpgrade(admin, feature, alterOptions.upgradeLevel);
            }

            // Handle downgrade
            if (alterOptions.downgradeLevel != null) {
                return handleDowngrade(admin, feature, alterOptions.downgradeLevel);
            }

            // Handle disable
            if (alterOptions.disable) {
                return handleDisable(admin, feature);
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to alter feature: " + e.getMessage());
            return 1;
        }
    }

    private int handleUpgrade(Admin admin, FeatureInfo feature, short targetLevel) throws Exception {
        // Validation
        if (feature.finalizedMaxVersion() != null && targetLevel <= feature.finalizedMaxVersion()) {
            err().println("Error: Target level " + targetLevel +
                " must be greater than current finalized level " + feature.finalizedMaxVersion());
            return 1;
        }

        if (feature.supportedMaxVersion() != null && targetLevel > feature.supportedMaxVersion()) {
            err().println("Error: Target level " + targetLevel +
                " exceeds maximum supported level " + feature.supportedMaxVersion());
            return 1;
        }

        // Execute upgrade
        featureService.updateFeature(admin, featureName, targetLevel, false);
        out().println("Upgraded feature \"" + featureName + "\" to level " + targetLevel);

        return 0;
    }

    private int handleDowngrade(Admin admin, FeatureInfo feature, short targetLevel) throws Exception {
        // Validation
        if (feature.finalizedMaxVersion() == null) {
            err().println("Error: Feature \"" + featureName + "\" is not finalized and cannot be downgraded.");
            return 1;
        }

        if (targetLevel >= feature.finalizedMaxVersion()) {
            err().println("Error: Target level " + targetLevel +
                " must be less than current finalized level " + feature.finalizedMaxVersion());
            return 1;
        }

        if (feature.supportedMinVersion() != null && targetLevel < feature.supportedMinVersion()) {
            err().println("Error: Target level " + targetLevel +
                " is below minimum supported level " + feature.supportedMinVersion());
            return 1;
        }

        // Confirmation prompt
        if (!autoConfirm) {
            out().println("WARNING: Downgrading features can cause cluster instability.");
            out().print("Downgrade feature \"" + featureName + "\" from " +
                feature.finalizedMaxVersion() + " to " + targetLevel + "? [y/N]: ");
            out().flush();

            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                out().println("Downgrade cancelled.");
                return 0;
            }
        }

        // Execute downgrade
        featureService.updateFeature(admin, featureName, targetLevel, true);
        out().println("Downgraded feature \"" + featureName + "\" to level " + targetLevel);

        return 0;
    }

    private int handleDisable(Admin admin, FeatureInfo feature) throws Exception {
        // Validation
        if (feature.finalizedMaxVersion() == null) {
            err().println("Error: Feature \"" + featureName + "\" is not finalized and cannot be disabled.");
            return 1;
        }

        // Confirmation prompt
        if (!autoConfirm) {
            out().println("WARNING: Disabling features can cause cluster instability.");
            out().print("Disable feature \"" + featureName + "\"? This cannot be undone. [y/N]: ");
            out().flush();

            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                out().println("Disable cancelled.");
                return 0;
            }
        }

        // Execute disable
        featureService.disableFeature(admin, featureName);
        out().println("Disabled feature \"" + featureName + "\"");

        return 0;
    }
}
