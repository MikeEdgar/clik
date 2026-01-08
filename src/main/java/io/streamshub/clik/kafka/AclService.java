package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.jboss.logging.Logger;

import io.streamshub.clik.kafka.model.AclInfo;

@ApplicationScoped
public class AclService {

    @Inject
    Logger logger;

    /**
     * Create ACL bindings
     */
    public void createAcls(Admin admin, Collection<AclBinding> aclBindings)
            throws ExecutionException, InterruptedException {

        admin.createAcls(aclBindings)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .get();
    }

    /**
     * List ACLs matching the filter
     */
    public List<AclInfo> listAcls(Admin admin, AclBindingFilter filter)
            throws ExecutionException, InterruptedException {

        Collection<AclBinding> bindings = admin.describeAcls(filter)
                .values()
                .get();

        return bindings.stream()
                .map(this::convertToAclInfo)
                // Sort ACLs for consistent output
                .sorted(AclInfo.comparator())
                .toList();
    }

    /**
     * Delete ACLs matching the filters
     * Returns collection of deleted ACL bindings
     */
    public Collection<AclInfo> deleteAcls(Admin admin, Collection<AclBindingFilter> filters)
            throws ExecutionException, InterruptedException {

        DeleteAclsResult result = admin.deleteAcls(filters);

        // Collect all deleted bindings
        List<AclInfo> deleted = new ArrayList<>();
        for (AclBindingFilter filter : filters) {
            Collection<AclBinding> matchedBindings = result.values()
                    .get(filter)
                    .get()
                    .values()
                    .stream()
                    .map(FilterResult::binding)
                    .toList();

            deleted.addAll(matchedBindings.stream()
                    .map(this::convertToAclInfo)
                    .toList());
        }

        return deleted;
    }

    /**
     * Convert Kafka AclBinding to domain model
     */
    private AclInfo convertToAclInfo(AclBinding binding) {
        ResourcePattern pattern = binding.pattern();
        AccessControlEntry entry = binding.entry();

        return AclInfo.builder()
                .resourceType(pattern.resourceType().toString())
                .resourceName(pattern.name())
                .patternType(pattern.patternType().toString())
                .principal(entry.principal())
                .host(entry.host())
                .operation(entry.operation().toString())
                .permissionType(entry.permissionType().toString())
                .build();
    }

    /**
     * Build AclBinding from components
     */
    public AclBinding buildAclBinding(
            String resourceType,
            String resourceName,
            String patternType,
            String principal,
            String host,
            String operation,
            String permissionType) {

        ResourcePattern pattern = new ResourcePattern(
                ResourceType.fromString(resourceType),
                resourceName,
                PatternType.fromString(patternType)
        );

        AccessControlEntry entry = new AccessControlEntry(
                principal,
                host,
                AclOperation.fromString(operation),
                AclPermissionType.fromString(permissionType)
        );

        return new AclBinding(pattern, entry);
    }

    /**
     * Build AclBindingFilter for queries
     * Any null parameter will be treated as ANY (wildcard)
     */
    public AclBindingFilter buildAclBindingFilter(
            String resourceType,
            String resourceName,
            String patternType,
            String principal,
            String host,
            String operation,
            String permissionType) {

        ResourcePatternFilter patternFilter = new ResourcePatternFilter(
                resourceType != null ? ResourceType.fromString(resourceType) : ResourceType.ANY,
                resourceName,
                patternType != null ? PatternType.fromString(patternType) : PatternType.ANY
        );

        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(
                principal,
                host,
                operation != null ? AclOperation.fromString(operation) : AclOperation.ANY,
                permissionType != null ? AclPermissionType.fromString(permissionType) : AclPermissionType.ANY
        );

        return new AclBindingFilter(patternFilter, entryFilter);
    }
}
