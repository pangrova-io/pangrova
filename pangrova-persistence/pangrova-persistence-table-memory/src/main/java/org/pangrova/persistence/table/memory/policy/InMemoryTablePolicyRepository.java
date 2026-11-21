package org.pangrova.persistence.table.memory.policy;

import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.table.api.policy.TablePolicyRepository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local binding store used when no durable metadata database is configured. */
public final class InMemoryTablePolicyRepository
        implements TablePolicyRepository {

    private final Map<Key, GovernanceBinding> bindings = new ConcurrentHashMap<>();

    @Override
    public GovernanceBinding save(GovernanceBinding binding) {
        return bindings.compute(new Key(binding.tableId(), binding.bindingType(), binding.bindingRef()),
                (key, existing) -> existing == null ? binding
                        : existing.withConfig(binding.config(), binding.updatedAt()));
    }

    @Override
    public Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type,
            String bindingRef) {
        return Optional.ofNullable(bindings.get(new Key(tableId, type, bindingRef)));
    }

    @Override
    public List<GovernanceBinding> list(UUID tableId) {
        List<GovernanceBinding> result = new ArrayList<>();
        for (GovernanceBinding binding : bindings.values()) {
            if (binding.tableId().equals(tableId)) {
                result.add(binding);
            }
        }
        result.sort(Comparator.comparing(GovernanceBinding::bindingType)
                .thenComparing(GovernanceBinding::bindingRef));
        return List.copyOf(result);
    }

    @Override
    public void delete(UUID tableId, GovernanceBindingType type, String bindingRef) {
        bindings.remove(new Key(tableId, type, bindingRef));
    }

    @Override
    public void deleteAll(UUID tableId) {
        bindings.keySet().removeIf(key -> key.tableId.equals(tableId));
    }

    private record Key(UUID tableId, GovernanceBindingType type, String bindingRef) { }
}
