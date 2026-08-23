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
                err().printf("Error: Feature \"%s\" not found.%n", featureName);
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

    private int handleUpgrade(Admin admin, FeatureInfo feature, short targetLevel) {
        // Validation
        if (feature.finalizedMaxVersion() != null && targetLevel <= feature.finalizedMaxVersion()) {
            err().printf("Error: Target level %s must be greater than current finalized level %s%n", 
                    targetLevel,
                    feature.finalizedMaxVersion());
            return 1;
        }

        if (feature.supportedMaxVersion() != null && targetLevel > feature.supportedMaxVersion()) {
            err().printf("Error: Target level %s exceeds maximum supported level %s%n",
                    targetLevel,
                    feature.supportedMaxVersion());
            return 1;
        }

        // Execute upgrade
        featureService.updateFeature(admin, featureName, targetLevel, false);
        out().printf("Upgraded feature \"%s\" to level %s%n", featureName, targetLevel);

        return 0;
    }

    private int handleDowngrade(Admin admin, FeatureInfo feature, short targetLevel) {
        // Validation
        if (feature.finalizedMaxVersion() == null) {
            err().printf("Error: Feature \"%s\" is not finalized and cannot be downgraded.%n", featureName);
            return 1;
        }

        if (targetLevel >= feature.finalizedMaxVersion()) {
            err().printf("Error: Target level %s must be less than current finalized level %s%n",
                    targetLevel,
                    feature.finalizedMaxVersion());
            return 1;
        }

        if (feature.supportedMinVersion() != null && targetLevel < feature.supportedMinVersion()) {
            err().printf("Error: Target level %s is below minimum supported level %s%n",
                    targetLevel,
                    feature.supportedMinVersion());
            return 1;
        }

        // Confirmation prompt
        if (!autoConfirm) {
            out().println("WARNING: Downgrading features can cause cluster instability.");
            out().printf("Downgrade feature \"%s\" from %s to %s? [y/N]: ",
                    featureName,
                    feature.finalizedMaxVersion(),
                    targetLevel);
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
        out().printf("Downgraded feature \"%s\" to level %s%n", featureName, targetLevel);

        return 0;
    }

    private int handleDisable(Admin admin, FeatureInfo feature) {
        // Validation
        if (feature.finalizedMaxVersion() == null) {
            err().printf("Error: Feature \"%s\" is not finalized and cannot be disabled.%n", featureName);
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
        out().printf("Disabled feature \"%s\"%n", featureName);

        return 0;
    }
}
