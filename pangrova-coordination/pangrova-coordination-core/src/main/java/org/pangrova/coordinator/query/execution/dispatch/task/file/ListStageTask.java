package org.pangrova.coordinator.query.execution.dispatch.task.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.access.FileListingClient;
import org.pangrova.file.core.space.FileSpaceAccessService;
import org.pangrova.file.api.access.FileAccessGrant;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ListStageStatement;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.file.api.directory.FileListing;
import org.pangrova.file.api.directory.FileStat;

/**
 * Executes {@code LIST @stage[/path]}; the Coordinator issues one read ticket and the
 * Worker enumerates objects, so the Coordinator never lists provider storage directly.
 */
public final class ListStageTask implements MetadataTask<ListStageStatement> {

    private static final List<String> COLUMNS =
            List.of("name", "size", "md5", "last_modified");
    private static final int MAX_PAGES = 10_000;

    private final FileSpaceAccessService access;
    private final FileListingClient client;

    public ListStageTask(FileSpaceAccessService access, FileListingClient client) {
        this.access = access;
        this.client = client;
    }

    @Override
    public StatementResult execute(ListStageStatement statement,
            StatementExecutionContext context) {
        if (access == null || client == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                    "LIST @stage requires an internal file-plane signing secret");
        }
        QualifiedTableName ref = FileSpaceTaskSupport.resolve(statement.name(), context.session());
        UserIdentity user = FileSpaceTaskSupport.requireUser(context.session());
        Pattern pattern = pattern(statement.pattern());
        FileAccessGrant grant = access.grant(user, "stages", ref.catalog(), ref.schema(),
                ref.table(), statement.path().orElse(""), "list");
        List<TypedRow> rows = new ArrayList<>();
        String pageToken = null;
        int pages = 0;
        do {
            FileListing listing = client.list(grant, pageToken);
            for (FileStat file : listing.files()) {
                if (pattern == null || pattern.matcher(file.path()).matches()) {
                    rows.add(row(ref, file));
                }
            }
            pageToken = listing.nextPageToken();
            pages++;
            if (pageToken != null && pages >= MAX_PAGES) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "LIST @stage did not terminate after " + MAX_PAGES + " pages");
            }
        } while (pageToken != null);
        return new StatementResult(rows, COLUMNS, null, StatementCategory.QUERY);
    }

    private static TypedRow row(QualifiedTableName ref, FileStat file) {
        TypedRow.Builder row = TypedRow.builder();
        row.putString("name", "@" + ref.catalog() + "." + ref.schema() + "." + ref.table()
                + "/" + file.path());
        row.putLong("size", file.size());
        row.putString("md5", file.md5());
        row.putString("last_modified", file.lastModified().toString());
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
                    "LIST PATTERN is not a valid regular expression: " + failure.getMessage());
        }
    }
}
