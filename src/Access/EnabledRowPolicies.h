#pragma once

#include <Access/Common/RowPolicyDefs.h>
#include <Access/RowPolicy.h>
#include <base/types.h>
#include <Core/UUID.h>

#include <boost/container/flat_set.hpp>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>

#include <memory>
#include <unordered_map>
#include <vector>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


struct RowPolicyFilter
{
    ASTPtr expression;
    std::vector<RowPolicyPtr> policies;

    void optimize();
};


/// Provides fast access to row policies' conditions for a specific user and tables.
class EnabledRowPolicies
{
public:
    struct Params
    {
        UUID user_id;
        boost::container::flat_set<UUID> enabled_roles;

        auto toTuple() const { return std::tie(user_id, enabled_roles); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    EnabledRowPolicies();
    ~EnabledRowPolicies();

    /// Returns prepared filter for a specific table and operations.
    /// The function can return nullptr, that means there is no filters applied.
    /// The returned filter can be a combination of the filters defined by multiple row policies.
    RowPolicyFilter getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const;
    RowPolicyFilter getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type, const RowPolicyFilter & combine_with_filter) const;

private:
    friend class RowPolicyCache;
    explicit EnabledRowPolicies(const Params & params_);

    struct MixedFiltersKey
    {
        std::string_view database;
        std::string_view table_name;
        RowPolicyFilterType filter_type;

        auto toTuple() const { return std::tie(database, table_name, filter_type); }
        friend bool operator==(const MixedFiltersKey & left, const MixedFiltersKey & right) { return left.toTuple() == right.toTuple(); }
        friend bool operator!=(const MixedFiltersKey & left, const MixedFiltersKey & right) { return left.toTuple() != right.toTuple(); }
    };

    struct MixedFiltersResult
    {
        ASTPtr ast;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
        std::vector<RowPolicyPtr> policies;
    };

    struct Hash
    {
        size_t operator()(const MixedFiltersKey & key) const;
    };

    using MixedFiltersMap = std::unordered_map<MixedFiltersKey, MixedFiltersResult, Hash>;

    const Params params;
    mutable boost::atomic_shared_ptr<const MixedFiltersMap> mixed_filters;
};

}
