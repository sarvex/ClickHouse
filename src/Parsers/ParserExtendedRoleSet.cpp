#include <Parsers/ParserExtendedRoleSet.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Parsers/parseUserName.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace
{
    bool parseRoleNameOrID(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, bool parse_id, String & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!parse_id)
                return parseRoleName(pos, expected, ranges, res);

            if (!ParserKeyword{"ID"}.ignore(pos, expected, ranges))
                return false;
            if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected, ranges))
                return false;
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected, ranges))
                return false;
            String id = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected, ranges))
                return false;

            res = std::move(id);
            return true;
        });
    }


    bool parseBeforeExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        IParser::Ranges * ranges,
        bool id_mode,
        bool all_keyword_enabled,
        bool current_user_keyword_enabled,
        Strings & names,
        bool & all,
        bool & current_user)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool res_all = false;
            bool res_current_user = false;
            Strings res_names;
            while (true)
            {
                if (ParserKeyword{"NONE"}.ignore(pos, expected, ranges))
                {
                }
                else if (
                    current_user_keyword_enabled
                    && (ParserKeyword{"CURRENT_USER"}.ignore(pos, expected, ranges) || ParserKeyword{"currentUser"}.ignore(pos, expected, ranges)))
                {
                    if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected, ranges))
                    {
                        if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected, ranges))
                            return false;
                    }
                    res_current_user = true;
                }
                else if (all_keyword_enabled && ParserKeyword{"ALL"}.ignore(pos, expected, ranges))
                {
                    res_all = true;
                }
                else
                {
                    String name;
                    if (!parseRoleNameOrID(pos, expected, ranges, id_mode, name))
                        return false;
                    res_names.push_back(name);
                }

                if (!ParserToken{TokenType::Comma}.ignore(pos, expected, ranges))
                    break;
            }

            all = res_all;
            current_user = res_current_user;
            names = std::move(res_names);
            return true;
        });
    }

    bool parseExceptAndAfterExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        IParser::Ranges * ranges,
        bool id_mode,
        bool current_user_keyword_enabled,
        Strings & except_names,
        bool & except_current_user)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"EXCEPT"}.ignore(pos, expected, ranges))
                return false;

            bool dummy;
            return parseBeforeExcept(pos, expected, ranges, id_mode, false, current_user_keyword_enabled, except_names, dummy, except_current_user);
        });
    }
}


bool ParserExtendedRoleSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, ranges, id_mode, all_keyword, current_user_keyword, names, all, current_user))
        return false;

    parseExceptAndAfterExcept(pos, expected, ranges, id_mode, current_user_keyword, except_names, except_current_user);

    if (all)
        names.clear();

    auto result = std::make_shared<ASTExtendedRoleSet>();
    result->names = std::move(names);
    result->current_user = current_user;
    result->all = all;
    result->except_names = std::move(except_names);
    result->except_current_user = except_current_user;
    result->id_mode = id_mode;
    node = result;
    return true;
}

}
