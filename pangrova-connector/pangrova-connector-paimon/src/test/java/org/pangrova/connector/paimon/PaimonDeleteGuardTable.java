package org.pangrova.connector.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.FullTextSearchBuilder;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.VectorSearchBuilder;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Delegates SDK metadata and commits while making coordinator data access fail loudly. */
final class PaimonDeleteGuardTable implements DataTable {

    private static final long serialVersionUID = 1L;

    private final DataTable delegate;
    private final boolean rejectBuilder;
    private final Function<BatchWriteBuilder, BatchTableCommit> commits;

    PaimonDeleteGuardTable(Table table) {
        this(table, false, builder -> builder.newCommit());
    }

    PaimonDeleteGuardTable(Table table, boolean rejectBuilder,
                          Function<BatchWriteBuilder, BatchTableCommit> commits) {
        this.delegate = (DataTable) table;
        this.rejectBuilder = rejectBuilder;
        this.commits = commits;
    }

    static PaimonDeleteGuardTable rejectOperations(Table table) {
        return new PaimonDeleteGuardTable(table, true, builder -> builder.newCommit());
    }

    @Override
    public ReadBuilder newReadBuilder() {
        throw forbidden("newReadBuilder");
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        throw forbidden("newSnapshotReader");
    }

    @Override
    public DataTableScan newScan() {
        throw forbidden("newScan");
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        throw forbidden("newStreamScan");
    }

    @Override
    public InnerTableRead newRead() {
        throw forbidden("newRead");
    }

    @Override
    public VectorSearchBuilder newVectorSearchBuilder() {
        throw forbidden("newVectorSearchBuilder");
    }

    @Override
    public FullTextSearchBuilder newFullTextSearchBuilder() {
        throw forbidden("newFullTextSearchBuilder");
    }

    @Override
    public BatchWriteBuilder newBatchWriteBuilder() {
        if (rejectBuilder) {
            throw forbidden("newBatchWriteBuilder before predicate rejection");
        }
        return new MetadataWriteBuilder(delegate.newBatchWriteBuilder(), commits);
    }

    @Override
    public StreamWriteBuilder newStreamWriteBuilder() {
        throw forbidden("newStreamWriteBuilder");
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        throw forbidden("newWriteSelector");
    }

    @Override
    public InnerTableWrite newWrite(String commitUser) {
        throw forbidden("newWrite");
    }

    @Override
    public InnerTableCommit newCommit(String commitUser) {
        throw forbidden("newCommit outside the batch builder");
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public String fullName() {
        return delegate.fullName();
    }

    @Override
    public String uuid() {
        return delegate.uuid();
    }

    @Override
    public RowType rowType() {
        return delegate.rowType();
    }

    @Override
    public List<String> partitionKeys() {
        return delegate.partitionKeys();
    }

    @Override
    public List<String> primaryKeys() {
        return delegate.primaryKeys();
    }

    @Override
    public Map<String, String> options() {
        return delegate.options();
    }

    @Override
    public Optional<String> comment() {
        return delegate.comment();
    }

    @Override
    public Optional<Statistics> statistics() {
        return delegate.statistics();
    }

    @Override
    public FileIO fileIO() {
        return delegate.fileIO();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new PaimonDeleteGuardTable(delegate.copy(dynamicOptions), rejectBuilder, commits);
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return delegate.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return delegate.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return delegate.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return delegate.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return delegate.indexManifestFileReader();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        delegate.rollbackTo(snapshotId);
    }

    @Override
    public void rollbackTo(String tagName) {
        delegate.rollbackTo(tagName);
    }

    @Override
    public void rollbackSchema(long schemaId) {
        delegate.rollbackSchema(schemaId);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        delegate.createTag(tagName, fromSnapshotId);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, @Nullable Duration timeRetained) {
        delegate.createTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public void createTag(String tagName) {
        delegate.createTag(tagName);
    }

    @Override
    public void createTag(String tagName, @Nullable Duration timeRetained) {
        delegate.createTag(tagName, timeRetained);
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        delegate.renameTag(tagName, targetTagName);
    }

    @Override
    public void replaceTag(String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained) {
        delegate.replaceTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public void deleteTag(String tagName) {
        delegate.deleteTag(tagName);
    }

    @Override
    public void createBranch(String branchName) {
        delegate.createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        delegate.createBranch(branchName, tagName);
    }

    @Override
    public void createBranch(String branchName, boolean ignoreIfExists) {
        delegate.createBranch(branchName, ignoreIfExists);
    }

    @Override
    public void createBranch(String branchName, String tagName, boolean ignoreIfExists) {
        delegate.createBranch(branchName, tagName, ignoreIfExists);
    }

    @Override
    public void deleteBranch(String branchName) {
        delegate.deleteBranch(branchName);
    }

    @Override
    public void renameBranch(String fromBranch, String toBranch) {
        delegate.renameBranch(fromBranch, toBranch);
    }

    @Override
    public void fastForward(String branchName) {
        delegate.fastForward(branchName);
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        return delegate.newExpireSnapshots();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return delegate.newExpireChangelog();
    }

    @Override
    public CoreOptions coreOptions() {
        return delegate.coreOptions();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return delegate.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return delegate.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return delegate.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return delegate.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return delegate.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return delegate.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new PaimonDeleteGuardTable(delegate.switchToBranch(branchName), rejectBuilder, commits);
    }

    @Override
    public Path location() {
        return delegate.location();
    }

    private static AssertionError forbidden(String operation) {
        return new AssertionError("Metadata DELETE must not call " + operation);
    }

    private static final class MetadataWriteBuilder implements BatchWriteBuilder {

        private static final long serialVersionUID = 1L;

        private final BatchWriteBuilder delegate;
        private final Function<BatchWriteBuilder, BatchTableCommit> commits;

        private MetadataWriteBuilder(BatchWriteBuilder delegate,
                                     Function<BatchWriteBuilder, BatchTableCommit> commits) {
            this.delegate = delegate;
            this.commits = commits;
        }

        @Override
        public String tableName() {
            return delegate.tableName();
        }

        @Override
        public RowType rowType() {
            return delegate.rowType();
        }

        @Override
        public Optional<WriteSelector> newWriteSelector() {
            throw forbidden("builder.newWriteSelector");
        }

        @Override
        public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
            delegate.withOverwrite(staticPartition);
            return this;
        }

        @Override
        public BatchTableWrite newWrite() {
            throw forbidden("builder.newWrite");
        }

        @Override
        public BatchTableCommit newCommit() {
            return commits.apply(delegate);
        }
    }
}