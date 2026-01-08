package io.streamshub.clik.command.acl;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.AclService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class AclCommandTest extends ClikMainTestBase {

    private AtomicBoolean initialized = new AtomicBoolean(false);

    QuarkusMainLauncher launcher;
    ContextService contextService;
    AclService aclService;
    TopicService topicService;

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        if (initialized.compareAndSet(false, true)) {
            // Run the application to trigger the startup of the devservices Kafka instance
            launcher.launch();
        }

        this.contextService = new ContextService(xdgConfigHome().toString());
        this.aclService = new AclService();
        this.topicService = new TopicService();

        // Create and set a test context
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", kafkaBootstrapServers())
                .build(), false);
        contextService.setCurrentContext("test-context");
    }

    // ============================
    // CREATE ACL TESTS (10 tests)
    // ============================

    @Test
    void testCreateAclBasicTopic() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "READ",
                "--principal", "User:alice");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclBasicGroup() {
        LaunchResult result = launcher.launch("acl", "create",
                "--group", "my-group",
                "--operation", "READ",
                "--principal", "User:bob");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclBasicCluster() {
        LaunchResult result = launcher.launch("acl", "create",
                "--cluster",
                "--operation", "ALTER",
                "--principal", "User:admin");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclPrefixedPattern() throws Exception {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "orders",
                "--pattern-type", "PREFIXED",
                "--operation", "READ",
                "--principal", "User:alice");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));

        var acls = aclService.listAcls(admin(), AclBindingFilter.ANY);
        assertEquals(1, acls.size());
        assertEquals("PREFIXED", acls.get(0).patternType());
    }

    @Test
    void testCreateAclDenyPermission() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "sensitive",
                "--operation", "WRITE",
                "--principal", "User:guest",
                "--permission", "DENY");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclCustomHost() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "READ",
                "--principal", "User:alice",
                "--host", "192.168.1.100");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclTransactionalId() {
        LaunchResult result = launcher.launch("acl", "create",
                "--transactional-id", "my-transaction",
                "--operation", "WRITE",
                "--principal", "User:producer");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclInvalidPrincipal() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "READ",
                "--principal", "alice"); // Missing "User:" prefix

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid principal format"));
        assertTrue(result.getErrorOutput().contains("User:"));
    }

    @Test
    void testCreateAclInvalidOperation() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "INVALID_OP",
                "--principal", "User:alice");

        assertEquals(2, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid value for option '--operation'"));
    }

    @Test
    void testCreateAclNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "READ",
                "--principal", "User:alice");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    // ============================
    // LIST ACLs TESTS (9 tests)
    // ============================

    @Test
    void testListAclsEmpty() {
        LaunchResult result = launcher.launch("acl", "list");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("No ACLs found"));
    }

    @Test
    void testListAclsTable() throws Exception {
        // Create test topic first
        topicService.createTopic(admin(), "list-test-topic", 1, 1, Collections.emptyMap());

        // Create some ACLs
        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "list-test-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "list-test-topic", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.WRITE.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "list");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("User:alice"));
        assertTrue(output.contains("User:bob"));
        assertTrue(output.contains("TOPIC"));
        assertTrue(output.contains("list-test-topic"));
        assertTrue(output.contains("READ"));
        assertTrue(output.contains("WRITE"));
        assertTrue(output.contains("PRINCIPAL"));
        assertTrue(output.contains("RESOURCE TYPE"));
    }

    @Test
    void testListAclsJsonFormat() throws Exception {
        // Create test topic first
        topicService.createTopic(admin(), "json-topic", 1, 1, Collections.emptyMap());

        var binding = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "json-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), Collections.singleton(binding));

        LaunchResult result = launcher.launch("acl", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"resourceType\""));
        assertTrue(output.contains("\"TOPIC\""));
        assertTrue(output.contains("\"json-topic\""));
        assertTrue(output.contains("\"principal\""));
        assertTrue(output.contains("\"User:alice\""));
        assertTrue(output.contains("\"operation\""));
        assertTrue(output.contains("\"READ\""));
    }

    @Test
    void testListAclsYamlFormat() throws Exception {
        // Create test topic first
        topicService.createTopic(admin(), "yaml-topic", 1, 1, Collections.emptyMap());

        var binding = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "yaml-topic", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.WRITE.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), Collections.singleton(binding));

        LaunchResult result = launcher.launch("acl", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("resourceType:"));
        assertTrue(output.contains("TOPIC"));
        assertTrue(output.contains("yaml-topic"));
        assertTrue(output.contains("principal:"));
        assertTrue(output.contains("User:bob"));
        assertTrue(output.contains("operation:"));
        assertTrue(output.contains("WRITE"));
    }

    @Test
    void testListAclsFilterByTopic() throws Exception {
        // Create two test topics
        topicService.createTopic(admin(), "filter-topic-1", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "filter-topic-2", 1, 1, Collections.emptyMap());

        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "filter-topic-1", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "filter-topic-2", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "list", "--topic", "filter-topic-1");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("filter-topic-1"));
        assertTrue(output.contains("User:alice"));
        assertFalse(output.contains("filter-topic-2"));
        assertFalse(output.contains("User:bob"));
    }

    @Test
    void testListAclsFilterByPrincipal() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "principal-topic", 1, 1, Collections.emptyMap());

        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "principal-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "principal-topic", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.WRITE.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "list", "--principal", "User:alice");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("User:alice"));
        assertFalse(output.contains("User:bob"));
    }

    @Test
    void testListAclsFilterByOperation() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "operation-topic", 1, 1, Collections.emptyMap());

        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "operation-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "operation-topic", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.WRITE.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "list", "--operation", "READ");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("READ"));
        assertTrue(output.contains("User:alice"));
        assertFalse(output.contains("WRITE"));
    }

    @Test
    void testListAclsMultipleFilters() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "multi-filter-topic", 1, 1, Collections.emptyMap());

        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "multi-filter-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "multi-filter-topic", PatternType.LITERAL.name(),
                "User:bob", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "list",
                "--topic", "multi-filter-topic",
                "--principal", "User:alice",
                "--operation", "READ");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("User:alice"));
        assertTrue(output.contains("multi-filter-topic"));
        assertTrue(output.contains("READ"));
        assertFalse(output.contains("User:bob"));
    }

    @Test
    void testListAclsNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("acl", "list");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    // ============================
    // DELETE ACL TESTS (7 tests)
    // ============================

    @Test
    void testDeleteAclSingle() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "delete-topic", 1, 1, Collections.emptyMap());

        var binding = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "delete-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), Collections.singleton(binding));

        LaunchResult result = launcher.launch("acl", "delete",
                "--topic", "delete-topic",
                "--principal", "User:alice",
                "--operation", "READ",
                "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 ACL binding deleted"));
    }

    @Test
    void testDeleteAclMultiple() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "multi-delete-topic", 1, 1, Collections.emptyMap());

        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "multi-delete-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "multi-delete-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.WRITE.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        LaunchResult result = launcher.launch("acl", "delete",
                "--topic", "multi-delete-topic",
                "--principal", "User:alice",
                "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("2 ACL bindings deleted"));
    }

    @Test
    @Disabled("Delete without --yes will cause clik to hang waiting for input")
    void testDeleteAclConfirmationPrompt() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "confirm-topic", 1, 1, Collections.emptyMap());

        var binding = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "confirm-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), Collections.singleton(binding));

        // Without --yes flag, should show prompt (can't test interactively in unit tests)
        // This test just verifies the preview is shown
        LaunchResult result = launcher.launch("acl", "delete",
                "--topic", "confirm-topic",
                "--principal", "User:alice",
                "--operation", "READ");

        // This will fail due to no stdin input, but we can verify it attempted confirmation
        assertEquals(1, result.exitCode());
    }

    @Test
    void testDeleteAclNoMatches() throws Exception {
        // Create test topic without ACLs
        topicService.createTopic(admin(), "no-match-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("acl", "delete",
                "--topic", "no-match-topic",
                "--principal", "User:nonexistent",
                "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No ACLs matched"));
    }

    @Test
    void testDeleteAclNoFilters() {
        LaunchResult result = launcher.launch("acl", "delete", "--yes");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("At least one filter must be specified"));
    }

    @Test
    void testDeleteAclByPrincipalOnly() throws Exception {
        // Create test topics
        topicService.createTopic(admin(), "topic-a", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "topic-b", 1, 1, Collections.emptyMap());

        // Create ACLs for alice on both topics
        var binding1 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "topic-a", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        var binding2 = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "topic-b", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());

        aclService.createAcls(admin(), java.util.List.of(binding1, binding2));

        // Delete all ACLs for alice
        LaunchResult result = launcher.launch("acl", "delete",
                "--principal", "User:alice",
                "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("2 ACL bindings deleted"));
    }

    @Test
    void testDeleteAclNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("acl", "delete",
                "--topic", "some-topic",
                "--principal", "User:alice",
                "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    // ============================
    // EDGE CASE TESTS (5 tests)
    // ============================

    @Test
    void testCreateAclWildcardPrincipal() {
        LaunchResult result = launcher.launch("acl", "create",
                "--cluster",
                "--operation", "DESCRIBE",
                "--principal", "User:*");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("ACL created successfully"));
    }

    @Test
    void testCreateAclAllResourceTypes() {
        // Test all resource types can be used
        LaunchResult topic = launcher.launch("acl", "create",
                "--topic", "test", "--operation", "READ", "--principal", "User:test");
        assertEquals(0, topic.exitCode());

        LaunchResult group = launcher.launch("acl", "create",
                "--group", "test", "--operation", "READ", "--principal", "User:test");
        assertEquals(0, group.exitCode());

        LaunchResult cluster = launcher.launch("acl", "create",
                "--cluster", "--operation", "ALTER", "--principal", "User:test");
        assertEquals(0, cluster.exitCode());

        LaunchResult txn = launcher.launch("acl", "create",
                "--transactional-id", "test", "--operation", "WRITE", "--principal", "User:test");
        assertEquals(0, txn.exitCode());

        LaunchResult token = launcher.launch("acl", "create",
                "--delegation-token", "test", "--operation", "DESCRIBE", "--principal", "User:test");
        assertEquals(0, token.exitCode());

        LaunchResult user = launcher.launch("acl", "create",
                "--user-resource", "test", "--operation", "ALTER", "--principal", "User:test");
        assertEquals(0, user.exitCode());
    }

    @Test
    void testListAclsInvalidOutputFormat() throws Exception {
        var binding = aclService.buildAclBinding(
                ResourceType.TOPIC.name(), "some-topic", PatternType.LITERAL.name(),
                "User:alice", "*", AclOperation.READ.name(), AclPermissionType.ALLOW.name());
        aclService.createAcls(admin(), Collections.singleton(binding));

        LaunchResult result = launcher.launch("acl", "list", "-o", "invalid");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Unknown output format"));
        assertTrue(result.getErrorOutput().contains("Valid formats: table, yaml, json"));
    }

    @Test
    void testCreateAclInvalidPermissionType() {
        LaunchResult result = launcher.launch("acl", "create",
                "--topic", "my-topic",
                "--operation", "READ",
                "--principal", "User:alice",
                "--permission", "INVALID");

        assertEquals(2, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid value for option '--permission'"));
    }
}
