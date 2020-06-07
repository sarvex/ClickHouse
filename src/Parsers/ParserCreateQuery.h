#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Poco/String.h>


namespace DB
{

/** A nested table. For example, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
    const char * getName() const override { return "nested table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


/** Parametric type or Storage. For example:
 *         FixedString(10) or
 *         Partitioned(Log, ChunkID) or
 *         Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
 * Result of parsing - ASTFunction with or without parameters.
 */
class ParserIdentifierWithParameters : public IParserBase
{
protected:
    const char * getName() const override { return "identifier with parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

template <typename NameParser>
class IParserNameTypePair : public IParserBase
{
protected:
    const char * getName() const  override{ return "name and type pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/** The name and type are separated by a space. For example, URL String. */
using ParserNameTypePair = IParserNameTypePair<ParserIdentifier>;
/** Name and type separated by a space. The name can contain a dot. For example, Hits.URL String. */
using ParserCompoundNameTypePair = IParserNameTypePair<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    NameParser name_parser;
    ParserIdentifierWithOptionalParameters type_parser;

    ASTPtr name, type;
    if (name_parser.parse(pos, name, expected, ranges)
        && type_parser.parse(pos, type, expected, ranges))
    {
        auto name_type_pair = std::make_shared<ASTNameTypePair>();
        tryGetIdentifierNameInto(name, name_type_pair->name);
        name_type_pair->type = type;
        name_type_pair->children.push_back(type);
        node = name_type_pair;
        return true;
    }

    return false;
}

/** List of columns. */
class ParserNameTypePairList : public IParserBase
{
protected:
    const char * getName() const override { return "name and type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/** List of table names. */
class ParserNameList : public IParserBase
{
protected:
    const char * getName() const override { return "name list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


template <typename NameParser>
class IParserColumnDeclaration : public IParserBase
{
public:
    explicit IParserColumnDeclaration(bool require_type_ = true) : require_type(require_type_)
    {
    }

protected:
    using ASTDeclarePtr = std::shared_ptr<ASTColumnDeclaration>;

    const char * getName() const  override{ return "column declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;

    bool require_type = true;
};

using ParserColumnDeclaration = IParserColumnDeclaration<ParserIdentifier>;
using ParserCompoundColumnDeclaration = IParserColumnDeclaration<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    NameParser name_parser;
    ParserIdentifierWithOptionalParameters type_parser;
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserKeyword s_ttl{"TTL"};
    ParserTernaryOperatorExpression expr_parser;
    ParserStringLiteral string_literal_parser;
    ParserCodec codec_parser;
    ParserExpression expression_parser;

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected, ranges))
        return false;

    /** column name should be followed by type name if it
      *    is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS, COMMENT}
      */
    ASTPtr type;
    String default_specifier;
    ASTPtr default_expression;
    ASTPtr comment_expression;
    ASTPtr codec_expression;
    ASTPtr ttl_expression;

    if (!s_default.checkWithoutMoving(pos, expected, ranges) &&
        !s_materialized.checkWithoutMoving(pos, expected, ranges) &&
        !s_alias.checkWithoutMoving(pos, expected, ranges) &&
        !s_comment.checkWithoutMoving(pos, expected, ranges) &&
        !s_codec.checkWithoutMoving(pos, expected, ranges))
    {
        if (!type_parser.parse(pos, type, expected, ranges))
            return false;
    }

    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected, ranges) || s_materialized.ignore(pos, expected, ranges) || s_alias.ignore(pos, expected, ranges))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected, ranges))
            return false;
    }

    if (require_type && !type && !default_expression)
        return false; /// reject column name without type


    if (s_comment.ignore(pos, expected, ranges))
    {
        /// should be followed by a string literal
        if (!string_literal_parser.parse(pos, comment_expression, expected, ranges))
            return false;
    }

    if (s_codec.ignore(pos, expected, ranges))
    {
        if (!codec_parser.parse(pos, codec_expression, expected, ranges))
            return false;
    }

    if (s_ttl.ignore(pos, expected, ranges))
    {
        if (!expression_parser.parse(pos, ttl_expression, expected, ranges))
            return false;
    }

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
    node = column_declaration;
    tryGetIdentifierNameInto(name, column_declaration->name);

    if (type)
    {
        column_declaration->type = type;
        column_declaration->children.push_back(std::move(type));
    }

    if (default_expression)
    {
        column_declaration->default_specifier = default_specifier;
        column_declaration->default_expression = default_expression;
        column_declaration->children.push_back(std::move(default_expression));
    }

    if (comment_expression)
    {
        column_declaration->comment = comment_expression;
        column_declaration->children.push_back(std::move(comment_expression));
    }

    if (codec_expression)
    {
        column_declaration->codec = codec_expression;
        column_declaration->children.push_back(std::move(codec_expression));
    }

    if (ttl_expression)
    {
        column_declaration->ttl = ttl_expression;
        column_declaration->children.push_back(std::move(ttl_expression));
    }

    return true;
}

class ParserColumnDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


/** name BY expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserIndexDeclaration : public IParserBase
{
public:
    ParserIndexDeclaration() {}

protected:
    const char * getName() const override { return "index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

class ParserConstraintDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

class ParserTablePropertyDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "table property (column, index, constraint) declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


class ParserIndexDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

class ParserConstraintDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


class ParserTablePropertiesDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "columns or indices declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


/**
  * ENGINE = name [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
class ParserStorage : public IParserBase
{
protected:
    const char * getName() const override { return "storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster]
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS ENGINE = engine SELECT ...
  *
  */
class ParserCreateTableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/// CREATE|ATTACH LIVE VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] AS SELECT ...
class ParserCreateLiveViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE LIVE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/// CREATE|ATTACH DATABASE db [ENGINE = engine]
class ParserCreateDatabaseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DATABASE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/// CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
class ParserCreateViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

/// Parses complete dictionary create query. Uses ParserDictionary and
/// ParserDictionaryAttributeDeclaration. Produces ASTCreateQuery.
/// CREATE DICTIONAY [IF NOT EXISTS] [db.]name (attrs) PRIMARY KEY key SOURCE(s(params)) LAYOUT(l(params)) LIFETIME([min v1 max] v2) [RANGE(min v1 max v2)]
class ParserCreateDictionaryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DICTIONARY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};


/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS ENGINE = engine SELECT ...
  *
  * Or:
  * CREATE|ATTACH DATABASE db [ENGINE = engine]
  *
  * Or:
  * CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;
};

}
