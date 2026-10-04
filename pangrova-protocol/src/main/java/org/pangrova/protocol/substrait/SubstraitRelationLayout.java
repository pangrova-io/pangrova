package org.pangrova.protocol.substrait;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.pangrova.spi.annotation.Internal;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** Sole schema and ordinal authority for canonical serialized relational plans. */
@Internal
public final class SubstraitRelationLayout {
    private SubstraitRelationLayout() {}

    /** Immutable metadata; expression context precedes the relation's own emit. */
    @Internal
    public record RelationLayout(List<NamedStruct> inputSchemas, NamedStruct expressionSchema,
                                 NamedStruct directSchema, NamedStruct outputSchema,
                                 List<Integer> outputMapping) {
        public RelationLayout {
            inputSchemas = List.copyOf(inputSchemas);
            outputMapping = List.copyOf(outputMapping);
        }

        /** Returns unique transport/operator identities without changing this layout's logical schema. */
        public NamedStruct physicalSchema() {
            return SubstraitRelationLayout.physicalSchema(outputSchema);
        }
    }

    /** Fragment-local index; lookup does not traverse child relations again. */
    @Internal
    public static final class LayoutTree {
        private final Map<Rel, RelationLayout> layouts;

        private LayoutTree(Map<Rel, RelationLayout> layouts) {
            this.layouts = new IdentityHashMap<>(layouts);
        }

        /**
         * Returns the immutable relation instances covered by this schema snapshot.
         * @return indexed relations; iteration order has no execution meaning
         */
        public List<Rel> relations() {
            return List.copyOf(layouts.keySet());
        }

        /**
         * Returns a layout for the same immutable relation instance indexed at construction.
         * @param relation the fragment relation
         * @return its canonical layout
         * @throws IllegalArgumentException if the relation is not in this fragment
         */
        public RelationLayout layout(Rel relation) {
            RelationLayout layout = layouts.get(relation);
            if (layout == null) throw new IllegalArgumentException("Relation is not in the indexed fragment");
            return layout;
        }
    }

    /**
     * Indexes a fragment once in post-order, including shared immutable children.
     * @param root canonical fragment root
     * @return identity-based schema index
     * @throws IllegalArgumentException for a malformed canonical relation
     */
    public static LayoutTree describeTree(Rel root) {
        Map<Rel, RelationLayout> layouts = new IdentityHashMap<>();
        visit(Objects.requireNonNull(root, "root"), layouts);
        return new LayoutTree(layouts);
    }

    /**
     * Replaces exact indexed relation instances without modifying other canonical fields.
     * @param root canonical fragment root
     * @param replacements identity-keyed replacement relations
     * @return immutable canonical tree with the selected relations replaced
     * @throws IllegalArgumentException for a relation without an active operator
     */
    public static Rel replaceRelations(Rel root, Map<Rel, Rel> replacements) {
        Map<Rel, Rel> indexed = new IdentityHashMap<>(replacements);
        return replace(root, indexed, new IdentityHashMap<>());
    }

    private static Rel replace(Rel relation, Map<Rel, Rel> replacements, Map<Rel, Rel> rewritten) {
        if (replacements.containsKey(relation)) return Objects.requireNonNull(replacements.get(relation));
        if (rewritten.containsKey(relation)) return rewritten.get(relation);
        Message original = payload(relation);
        var builder = original.toBuilder();
        for (var field : relationFields(original)) {
            if (field.isRepeated()) {
                for (int i = 0; i < original.getRepeatedFieldCount(field); i++) {
                    builder.setRepeatedField(field, i,
                            replace((Rel) original.getRepeatedField(field, i), replacements, rewritten));
                }
            } else if (original.hasField(field)) {
                builder.setField(field, replace((Rel) original.getField(field), replacements, rewritten));
            }
        }
        Rel result = relation.toBuilder().setField(activeField(relation), builder.build()).build();
        rewritten.put(relation, result);
        return result;
    }

    /**
     * Describes a root; repeated fragment consumers should use describeTree instead.
     * @param relation canonical root
     * @return root layout
     * @throws IllegalArgumentException for a malformed canonical relation
     */
    public static RelationLayout describe(Rel relation) {
        return describeTree(relation).layout(relation);
    }

    /**
     * Applies the root's explicit DFS labels to the relation's canonical output.
     * @param root serialized fragment root
     * @return the complete declared result schema
     * @throws IllegalArgumentException for missing or surplus DFS result names
     */
    public static NamedStruct rootSchema(RelRoot root) {
        if (!root.hasInput()) throw new IllegalArgumentException("RelRoot requires an input");
        var schema = NamedStruct.newBuilder().setStruct(describe(root.getInput())
                .outputSchema().getStruct()).addAllNames(root.getNamesList()).build();
        SubstraitSchemaFields.fields(schema);
        return schema;
    }

    /**
     * Resolves only declared/contextual types, never execution values or coercions.
     * @param expression expression to inspect
     * @param expressionSchema its canonical input context
     * @return exact physical type
     * @throws IllegalArgumentException for a missing or incompatible type/path
     */
    public static Type typeOf(Expression expression, NamedStruct expressionSchema) {
        return SubstraitExpressionTypes.typeOf(Objects.requireNonNull(expression, "expression"),
                Objects.requireNonNull(expressionSchema, "expressionSchema"));
    }

    /**
     * Creates an explicit incoming-record reference; no implicit root is valid on the wire.
     * @param ordinal zero-based incoming field position
     * @return canonical direct root field expression
     * @throws IllegalArgumentException for a negative ordinal
     */
    public static Expression rootFieldReference(int ordinal) {
        if (ordinal < 0) throw new IllegalArgumentException("Negative root field ordinal");
        return Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
                .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
                .setDirectReference(Expression.ReferenceSegment.newBuilder().setStructField(
                        Expression.ReferenceSegment.StructField.newBuilder().setField(ordinal)))).build();
    }

    /**
     * Reads root names while skipping every nested DFS name subtree.
     * @param schema complete NamedStruct
     * @return root field names in ordinal order
     * @throws IllegalArgumentException for incomplete/extra DFS names
     */
    public static List<String> topLevelNames(NamedStruct schema) {
        return SubstraitSchemaFields.fields(schema).stream().map(SubstraitSchemaFields.Field::name).toList();
    }

    /**
     * Reads an existing field name through the same complete DFS schema authority.
     * Collection paths use element 0, or map key/value 0/1; the terminal child
     * must be a ROW field. An empty path names the requested top-level field.
     *
     * @param schema the complete canonical named schema
     * @param topOrdinal top-level field ordinal
     * @param childOrdinals nested type-child ordinals
     * @return the source-owned field name, including an explicitly empty name
     * @throws IllegalArgumentException for an invalid path or incomplete DFS names
     */
    public static String nameAtTypePath(NamedStruct schema, int topOrdinal, List<Integer> childOrdinals) {
        var fields = SubstraitSchemaFields.fields(schema);
        if (childOrdinals == null || childOrdinals.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("Named field path requires explicit child ordinals");
        }
        if (topOrdinal < 0 || topOrdinal >= fields.size() || childOrdinals.size() > 64) {
            throw new IllegalArgumentException("Named field path exceeds its schema or depth bound");
        }
        var field = fields.get(topOrdinal);
        return childOrdinals.isEmpty() ? field.name()
                : SubstraitSchemaFields.nameAtPath(field.type(), field.descendants(), childOrdinals, 0, 0);
    }

    /** Resolves a registered named-row constraint against source-owned canonical DFS names. */
    public static String nameAtCanonicalTypePath(NamedStruct schema, int topOrdinal,
            List<org.pangrova.protocol.v2.FunctionTypePathStep> path) {
        if (!schema.hasStruct() || topOrdinal < 0 || topOrdinal >= schema.getStruct().getTypesCount()) {
            throw new IllegalArgumentException("Named canonical type path has no input field");
        }
        return nameAtTypePath(schema, topOrdinal,
                SubstraitSchemaFields.childOrdinals(schema.getStruct().getTypes(topOrdinal), path));
    }

    /**
     * Returns the unique task-local top-level identity for one input/output ordinal.
     * @param ordinal zero-based field position
     * @return physical field alias, unrelated to the source's logical name
     * @throws IllegalArgumentException for a negative ordinal
     */
    public static String physicalAlias(int ordinal) {
        if (ordinal < 0) throw new IllegalArgumentException("Negative physical field ordinal");
        return "$pangrova$column_" + ordinal;
    }

    /**
     * Aliases top-level identities only, preserving logical nested names and complete types.
     * @param logical complete logical schema
     * @return physical schema used by Native operators and typed Arrow exchange
     * @throws IllegalArgumentException for an incomplete NamedStruct
     */
    public static NamedStruct physicalSchema(NamedStruct logical) {
        var fields = SubstraitSchemaFields.fields(logical);
        List<SubstraitSchemaFields.Field> physical = new ArrayList<>();
        for (int ordinal = 0; ordinal < fields.size(); ordinal++) {
            var field = fields.get(ordinal);
            physical.add(new SubstraitSchemaFields.Field(physicalAlias(ordinal), field.type(), field.descendants()));
        }
        return SubstraitSchemaFields.schema(physical, logical.getStruct());
    }

    /**
     * Validates one output mapping; present empty emit is not direct output.
     * @param common relation output contract
     * @param directFields number of fields before emit
     * @return immutable ordered indices, preserving duplicates
     * @throws IllegalArgumentException for an invalid ordinal/count
     */
    public static List<Integer> outputMapping(RelCommon common, int directFields) {
        if (directFields < 0) throw new IllegalArgumentException("Negative direct field count");
        List<Integer> indices = common.hasEmit() ? common.getEmit().getOutputMappingList()
                : IntStream.range(0, directFields).boxed().toList();
        if (indices.stream().anyMatch(index -> index < 0 || index >= directFields)) {
            throw new IllegalArgumentException("Output mapping is out of bounds for " + directFields + " fields");
        }
        return List.copyOf(indices);
    }

    /**
     * Applies a root-only mapping without losing descendant names or types.
     * @param schema direct output schema
     * @param indices ordered field indices
     * @return complete remapped NamedStruct
     * @throws IllegalArgumentException for an invalid ordinal/schema
     */
    public static NamedStruct projectSchema(NamedStruct schema, List<Integer> indices) {
        var fields = SubstraitSchemaFields.fields(schema);
        if (indices.stream().anyMatch(index -> index < 0 || index >= fields.size())) {
            throw new IllegalArgumentException("Schema projection is out of bounds");
        }
        NamedStruct projected = SubstraitSchemaFields.schema(indices.stream().map(fields::get).toList());
        return projected.toBuilder().setStruct(schema.getStruct().toBuilder().clearTypes()
                .addAllTypes(projected.getStruct().getTypesList())).build();
    }

    /**
     * Appends explicitly declared computed fields using canonical anonymous DFS names.
     * No expression/type inference or source-name substitution is performed.
     * @param input exact existing logical schema
     * @param names top-level computed names, in order
     * @param declaredTypes exact declared types, in the same order
     * @return schema preserving the complete input names, types and root metadata
     * @throws IllegalArgumentException for inconsistent arity or invalid declared types
     */
    public static NamedStruct appendComputedFields(NamedStruct input, List<String> names,
            List<Type> declaredTypes) {
        if (names.size() != declaredTypes.size()) {
            throw new IllegalArgumentException("Computed names/types have inconsistent arity");
        }
        var fields = new ArrayList<>(SubstraitSchemaFields.fields(input));
        for (int i = 0; i < names.size(); i++) {
            SubstraitExpressionTypes.declared(declaredTypes.get(i));
            fields.add(SubstraitSchemaFields.computed(names.get(i), declaredTypes.get(i)));
        }
        return SubstraitSchemaFields.schema(fields, input.getStruct());
    }

    /**
     * Composes output transforms in execution order, preserving the latter hints.
     * @param first first relation's common output contract
     * @param second second relation's common output contract
     * @param directFields fields before the first transformation
     * @return a single equivalent output mapping
     * @throws IllegalArgumentException for an invalid mapping
     */
    public static RelCommon compose(RelCommon first, RelCommon second, int directFields) {
        var initial = outputMapping(first, directFields);
        var finalIndices = outputMapping(second, initial.size()).stream().map(initial::get).toList();
        return second.toBuilder().setEmit(RelCommon.Emit.newBuilder().addAllOutputMapping(finalIndices)).build();
    }

    /**
     * Preserves old outputs when new fields are appended to a Project's input.
     * @param common original output contract
     * @param inputFields original input arity
     * @param expressionFields appended expression arity, zero for pass-through relations
     * @param addedFields new input fields
     * @param emitAddedFields whether to append the new input fields to old outputs
     * @return adjusted mapping with original output ordinals unchanged
     * @throws IllegalArgumentException for invalid counts/mappings
     */
    public static RelCommon withAppendedInput(RelCommon common, int inputFields,
            int expressionFields, int addedFields, boolean emitAddedFields) {
        if (inputFields < 0 || expressionFields < 0 || addedFields < 0) {
            throw new IllegalArgumentException("Negative field count");
        }
        var mapping = outputMapping(common, Math.addExact(inputFields, expressionFields));
        var emit = RelCommon.Emit.newBuilder();
        for (int field : mapping) {
            emit.addOutputMapping(field < inputFields ? field : Math.addExact(field, addedFields));
        }
        if (emitAddedFields) {
            for (int i = 0; i < addedFields; i++) emit.addOutputMapping(Math.addExact(inputFields, i));
        }
        return common.toBuilder().setEmit(emit).build();
    }

    /**
     * Returns the canonical common message of an active relation.
     * @param relation active canonical relation
     * @return common, or its direct-output default if omitted
     * @throws IllegalArgumentException for an unset relation
     */
    public static RelCommon common(Rel relation) {
        Message payload = payload(relation);
        var field = payload.getDescriptorForType().findFieldByName("common");
        return field == null ? RelCommon.getDefaultInstance() : (RelCommon) payload.getField(field);
    }

    private static void visit(Rel relation, Map<Rel, RelationLayout> layouts) {
        if (layouts.containsKey(relation)) return;
        List<NamedStruct> inputs = new ArrayList<>();
        for (Rel child : children(payload(relation))) {
            visit(child, layouts);
            inputs.add(layouts.get(child).outputSchema());
        }
        NamedStruct context = relation.hasRead() ? relation.getRead().getBaseSchema()
                : inputs.size() == 1 ? inputs.get(0) : SubstraitRelationShapes.concatenate(inputs);
        NamedStruct direct = SubstraitRelationShapes.direct(relation, inputs, context);
        List<Integer> emit = outputMapping(common(relation), direct.getStruct().getTypesCount());
        layouts.put(relation, new RelationLayout(inputs, context, direct, projectSchema(direct, emit), emit));
    }

    static Message payload(Rel relation) {
        return (Message) relation.getField(activeField(relation));
    }

    private static FieldDescriptor activeField(Rel relation) {
        FieldDescriptor field = relation.getDescriptorForType()
                .findFieldByNumber(relation.getRelTypeCase().getNumber());
        if (field == null || !relation.hasField(field)) throw new IllegalArgumentException("Relation has no operator");
        return field;
    }

    private static List<Rel> children(Message payload) {
        List<Rel> children = new ArrayList<>();
        for (var field : relationFields(payload)) {
            if (field.isRepeated()) {
                for (int i = 0; i < payload.getRepeatedFieldCount(field); i++) {
                    children.add((Rel) payload.getRepeatedField(field, i));
                }
            } else if (payload.hasField(field)) {
                children.add((Rel) payload.getField(field));
            }
        }
        return children;
    }

    private static List<FieldDescriptor> relationFields(Message payload) {
        return payload.getDescriptorForType().getFields().stream()
                .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                        && field.getMessageType().getFullName().equals("substrait.Rel"))
                .toList();
    }
}
