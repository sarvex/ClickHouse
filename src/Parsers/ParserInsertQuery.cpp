#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTInsertQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserInsertQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_insert_into("INSERT INTO");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_function("FUNCTION");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_values("VALUES");
    ParserKeyword s_format("FORMAT");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_select("SELECT");
    ParserKeyword s_watch("WATCH");
    ParserKeyword s_with("WITH");
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserFunction table_function_p{false};

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr format;
    ASTPtr select;
    ASTPtr watch;
    ASTPtr table_function;
    ASTPtr settings_ast;
    /// Insertion data
    const char * data = nullptr;

    if (!s_insert_into.ignore(pos, expected, ranges))
        return false;

    s_table.ignore(pos, expected, ranges);

    if (s_function.ignore(pos, expected, ranges))
    {
        if (!table_function_p.parse(pos, table_function, expected, ranges))
            return false;
    }
    else
    {
        if (!name_p.parse(pos, table, expected, ranges))
            return false;

        if (s_dot.ignore(pos, expected, ranges))
        {
            database = table;
            if (!name_p.parse(pos, table, expected, ranges))
                return false;
        }
    }

    /// Is there a list of columns
    if (s_lparen.ignore(pos, expected, ranges))
    {
        if (!columns_p.parse(pos, columns, expected, ranges))
            return false;

        if (!s_rparen.ignore(pos, expected, ranges))
            return false;
    }

    Pos before_values = pos;

    /// VALUES or FORMAT or SELECT
    if (s_values.ignore(pos, expected, ranges))
    {
        data = pos->begin;
    }
    else if (s_format.ignore(pos, expected, ranges))
    {
        if (!name_p.parse(pos, format, expected, ranges))
            return false;
    }
    else if (s_select.ignore(pos, expected, ranges) || s_with.ignore(pos, expected, ranges))
    {
        pos = before_values;
        ParserSelectWithUnionQuery select_p;
        select_p.parse(pos, select, expected, ranges);

        /// FORMAT section is expected if we have input() in SELECT part
        if (s_format.ignore(pos, expected, ranges) && !name_p.parse(pos, format, expected, ranges))
            return false;
    }
    else if (s_watch.ignore(pos, expected, ranges))
    {
        pos = before_values;
        ParserWatchQuery watch_p;
        watch_p.parse(pos, watch, expected, ranges);

        /// FORMAT section is expected if we have input() in SELECT part
        if (s_format.ignore(pos, expected, ranges) && !name_p.parse(pos, format, expected, ranges))
            return false;
    }
    else
    {
        return false;
    }

    if (s_settings.ignore(pos, expected, ranges))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings_ast, expected, ranges))
            return false;
    }

    if (format)
    {
        Pos last_token = pos;
        --last_token;
        data = last_token->end;

        if (data < end && *data == ';')
            throw Exception("You have excessive ';' symbol before data for INSERT.\n"
                                    "Example:\n\n"
                                    "INSERT INTO t (x, y) FORMAT TabSeparated\n"
                                    ";\tHello\n"
                                    "2\tWorld\n"
                                    "\n"
                                    "Note that there is no ';' just after format name, "
                                    "you need to put at least one whitespace symbol before the data.", ErrorCodes::SYNTAX_ERROR);

        while (data < end && (*data == ' ' || *data == '\t' || *data == '\f'))
            ++data;

        /// Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.

        if (data < end && *data == '\r')
            ++data;

        if (data < end && *data == '\n')
            ++data;
    }

    auto query = std::make_shared<ASTInsertQuery>();
    node = query;

    if (table_function)
    {
        query->table_function = table_function;
    }
    else
    {
        tryGetIdentifierNameInto(database, query->table_id.database_name);
        tryGetIdentifierNameInto(table, query->table_id.table_name);
    }

    tryGetIdentifierNameInto(format, query->format);

    query->columns = columns;
    query->select = select;
    query->watch = watch;
    query->settings_ast = settings_ast;
    query->data = data != end ? data : nullptr;
    query->end = end;

    if (columns)
        query->children.push_back(columns);
    if (select)
        query->children.push_back(select);
    if (watch)
        query->children.push_back(watch);
    if (settings_ast)
        query->children.push_back(settings_ast);

    return true;
}


}
