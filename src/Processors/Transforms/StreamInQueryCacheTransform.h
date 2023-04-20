#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Cache/QueryCache.h>

namespace DB
{

class StreamInQueryCacheTransform : public ISimpleTransform
{
public:
    enum class Type {Out, Totals, Extremes};

    StreamInQueryCacheTransform(
        const Block & header_,
        std::shared_ptr<QueryCache::Writer> query_cache_writer,
        Type type);

protected:
    void transform(Chunk & chunk) override;

public:
    void finalizeWriteInQueryCache();
    String getName() const override { return "StreamInQueryCacheTransform"; }

private:
    std::shared_ptr<QueryCache::Writer> query_cache_writer;
    Type type;
};

}
