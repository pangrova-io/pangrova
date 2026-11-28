package org.pangrova.coordinator.query.execution.dispatch.task.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.access.FileDeletionClient;
import org.pangrova.file.api.access.FileListingClient;
import org.pangrova.file.access.FileSpaceAccessService;
import org.pangrova.file.api.access.FileAccessGrant;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.RemoveStageStatement;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;

/**
 * Executes {@code REMOVE @stage[/path] [PATTERN = 'regex']}.
 *
 * <p>The Coordinator issues one listing ticket for the prefix and one delete ticket
 * per object, so provider credentials stay in the Worker and every removal is
 * authorized and audited against its exact path. The complete match set is
 * collected before the first delete: a listing failure must not leave a
 * half-removed prefix, and deleting cannot invalidate a page token of an
 * in-flight listing.</p>
 */
public final class RemoveStageTask implements MetadataTask<RemoveStageStatement> {

    static final int MAX_PAGES = 10_000;
    static final int MAX_FILES = 100_000;
    private static final List<String> COLUMNS = List.of("name", "result");

    private final FileSpaceAccessService access;
    private final FileListingClient lister;
    private final FileDeletionClient deleter;

    public RemoveStageTask(FileSpaceAccessService access, FileListingClient lister,
            FileDeletionClient deleter) {
        this.access = access;
        this.lister = lister;
        this.deleter = deleter;
    }

    @Override
    public StatementResult execute(RemoveStageStatement statement,
            StatementExecutionContext context) {
        if (access == null || lister == null || deleter == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                    "REMOVE @stage requires an internal file-plane signing secret");
        }
        QualifiedTableName ref = FileSpaceTaskAuthority.resolve(statement.name(), context.session());
        UserIdentity user = FileSpaceTaskAuthority.requireUser(context.session());
        Pattern pattern = pattern(statement.pattern());
        FileAccessGrant listing = access.grant(user, "stages", ref.catalog(), ref.schema(),
                ref.table(), statement.path().orElse(""), "list");
        List<FileStat> matches = matches(listing, pattern);
        List<TypedRow> rows = new ArrayList<>(matches.size());
        for (FileStat file : matches) {
            FileAccessGrant deletion = access.grant(user, "stages", ref.catalog(), ref.schema(),
                    ref.table(), file.path(), "delete");
            rows.add(row(ref, file.path(), remove(deletion, file)));
        }
        return new StatementResult(rows, COLUMNS, null, StatementCategory.MAINTENANCE);
    }

    private List<FileStat> matches(FileAccessGrant grant, Pattern pattern) {
        List<FileStat> matches = new ArrayList<>();
        String pageToken = null;
        int pages = 0;
        do {
            FileListing listing = lister.list(grant, pageToken);
            for (FileStat file : listing.files()) {
                if (pattern != null && !pattern.matcher(file.path()).matches()) {
                    continue;
                }
                matches.add(file);
                if (matches.size() > MAX_FILES) {
                    throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                            "REMOVE matched more than " + MAX_FILES
                                    + " files; narrow the path or PATTERN");
                }
            }
            pageToken = listing.nextPageToken();
            pages++;
            if (pageToken != null && pages >= MAX_PAGES) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "REMOVE @stage did not terminate after " + MAX_PAGES + " pages");
            }
        } while (pageToken != null);
        return matches;
    }

    /**
     * Deletes one listed object and reports the outcome. A per-file failure is
     * reported in the row instead of discarding the rows of the files that were
     * already removed.
     */
    private String remove(FileAccessGrant grant, FileStat file) {
        String expectedVersion = file.providerVersion() != null
                ? file.providerVersion() : file.etag();
        try {
            deleter.delete(grant, expectedVersion);
            return "removed";
        } catch (PangrovaException failure) {
            return "error: " + failure.getMessage();
        }
    }

    private static TypedRow row(QualifiedTableName ref, String path, String result) {
        TypedRow.Builder row = TypedRow.builder();
        row.putString("name", "@" + ref.catalog() + "." + ref.schema() + "." + ref.table()
                + "/" + path);
        row.putString("result", result);
        return row.build();
    }

    private static Pattern pattern(Optional<String> expression) {
        if (expression.isEmpty()) {
            return null;
        }
        try {
            return Pattern.compile(expression.get());
        } catch (PatternSyntaxException failure) {
            throw new PangrovaException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "REMOVE PATTERN is not a valid regular expression: "
                            + failure.getMessage());
        }
    }
}
