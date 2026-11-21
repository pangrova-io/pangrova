/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.parser.build;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.build.antlr.PangrovaSqlParser;

import java.util.Optional;
import java.util.OptionalLong;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.SourceLocation;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateBranchStatement;
import org.pangrova.parser.api.ast.statement.CreateTagStatement;
import org.pangrova.parser.api.ast.statement.DropBranchStatement;
import org.pangrova.parser.api.ast.statement.DropTagStatement;
import org.pangrova.parser.api.ast.statement.ShowBranchesStatement;
import org.pangrova.parser.api.ast.statement.ShowTagsStatement;
import org.pangrova.parser.api.ast.statement.UseBranchStatement;

final class AstVersionedTableDdlBuilder {

    private final AstBuilder parent;

    AstVersionedTableDdlBuilder(AstBuilder parent) {
        this.parent = parent;
    }

    AlterTableStatement visitAlterTableCreateBranch(
            PangrovaSqlParser.AlterTableCreateBranchContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String branchName = AstDdlDmlBuilder.extractIdentifierText(ctx.branchName);
        OptionalLong retentionDays = ctx.retentionDays != null
                ? OptionalLong.of(Long.parseLong(ctx.retentionDays.getText()))
                : OptionalLong.empty();
        return new AlterTableStatement.CreateBranch(tableName, branchName, retentionDays);
    }

    AlterTableStatement visitAlterTableDropBranch(
            PangrovaSqlParser.AlterTableDropBranchContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String branchName = AstDdlDmlBuilder.extractIdentifierText(ctx.branchName);
        return new AlterTableStatement.DropBranch(tableName, branchName, ctx.EXISTS() != null);
    }

    AlterTableStatement visitAlterTableCreateTag(
            PangrovaSqlParser.AlterTableCreateTagContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String tagName = AstDdlDmlBuilder.extractIdentifierText(ctx.tagName);
        return new AlterTableStatement.CreateTag(tableName, tagName);
    }

    AlterTableStatement visitAlterTableDropTag(
            PangrovaSqlParser.AlterTableDropTagContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String tagName = AstDdlDmlBuilder.extractIdentifierText(ctx.tagName);
        return new AlterTableStatement.DropTag(tableName, tagName, ctx.EXISTS() != null);
    }

    AlterTableStatement visitAlterTableAdopt(
            PangrovaSqlParser.AlterTableAdoptContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        java.util.Map<String, String> properties = ctx.propertyList() == null
                ? java.util.Map.of()
                : parent.propertyTextMap(ctx.propertyList());
        return new AlterTableStatement.Adopt(tableName, properties);
    }

    AlterTableStatement visitAlterTableUnlink(
            PangrovaSqlParser.AlterTableUnlinkContext ctx) {
        return new AlterTableStatement.Unlink(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    AlterTableStatement visitAlterTableSetColumnTag(
            PangrovaSqlParser.AlterTableSetColumnTagContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String columnName = AstDdlDmlBuilder.extractIdentifierText(ctx.columnName);
        String tagKey = AstDdlDmlBuilder.extractIdentifierText(ctx.tagKey);
        String tagValue = AstDdlDmlBuilder.stripQuotes(ctx.tagValue.getText());
        return new AlterTableStatement.SetColumnTag(tableName, columnName, tagKey, tagValue);
    }

    AlterTableStatement visitAlterTableUnsetColumnTag(
            PangrovaSqlParser.AlterTableUnsetColumnTagContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String columnName = AstDdlDmlBuilder.extractIdentifierText(ctx.columnName);
        String tagKey = AstDdlDmlBuilder.extractIdentifierText(ctx.tagKey);
        return new AlterTableStatement.UnsetColumnTag(tableName, columnName, tagKey);
    }

    CreateBranchStatement visitCreateBranch(PangrovaSqlParser.CreateBranchContext ctx) {
        String branchName = AstDdlDmlBuilder.extractIdentifierText(ctx.branchName);
        Identifier tableName = parent.visitQualifiedName(ctx.tableName);
        Optional<String> fromTag = ctx.fromTag != null
                ? Optional.of(AstDdlDmlBuilder.extractIdentifierText(ctx.fromTag))
                : Optional.empty();
        return new CreateBranchStatement(branchName, tableName, fromTag,
                parseSnapshotId(ctx.fromSnapshot));
    }

    DropBranchStatement visitDropBranch(PangrovaSqlParser.DropBranchContext ctx) {
        String branchName = AstDdlDmlBuilder.extractIdentifierText(ctx.branchName);
        return new DropBranchStatement(branchName, parent.visitQualifiedName(ctx.tableName));
    }

    CreateTagStatement visitCreateTag(PangrovaSqlParser.CreateTagContext ctx) {
        String tagName = AstDdlDmlBuilder.extractIdentifierText(ctx.tagName);
        return new CreateTagStatement(tagName, parent.visitQualifiedName(ctx.tableName));
    }

    DropTagStatement visitDropTag(PangrovaSqlParser.DropTagContext ctx) {
        String tagName = AstDdlDmlBuilder.extractIdentifierText(ctx.tagName);
        return new DropTagStatement(tagName, parent.visitQualifiedName(ctx.tableName));
    }

    UseBranchStatement visitUseBranch(PangrovaSqlParser.UseBranchContext ctx) {
        String branchName = ctx.MAIN() != null
                ? "main"
                : AstDdlDmlBuilder.extractIdentifierText(ctx.branchName);
        return new UseBranchStatement(branchName, parent.visitQualifiedName(ctx.tableName));
    }

    ShowBranchesStatement visitShowBranches(PangrovaSqlParser.ShowBranchesContext ctx) {
        return new ShowBranchesStatement(parent.visitQualifiedName(ctx.tableName));
    }

    ShowTagsStatement visitShowTags(PangrovaSqlParser.ShowTagsContext ctx) {
        return new ShowTagsStatement(parent.visitQualifiedName(ctx.tableName));
    }

    private static OptionalLong parseSnapshotId(org.antlr.v4.runtime.Token snapshot) {
        if (snapshot == null) {
            return OptionalLong.empty();
        }
        String snapshotText = snapshot.getText();
        try {
            return OptionalLong.of(Long.parseLong(snapshotText));
        } catch (NumberFormatException e) {
            SourceLocation loc = new SourceLocation(snapshot.getLine(),
                    snapshot.getCharPositionInLine(), snapshot.getTokenIndex(),
                    snapshot.getTokenIndex());
            throw new ParseException(PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid snapshot ID: '" + snapshotText + "'", loc, e);
        }
    }
}
