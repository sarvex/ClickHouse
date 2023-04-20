#include <Processors/Transforms/StreamInQueryCacheTransform.h>

namespace DB
{

StreamInQueryCacheTransform::StreamInQueryCacheTransform(
    const Block & header_,
    std::shared_ptr<QueryCache::Writer> query_cache_writer_,
    Type type_)
    : ISimpleTransform(header_, header_, false)
    , query_cache_writer(query_cache_writer_)
    , type(type_)
{
}

void StreamInQueryCacheTransform::transform([[maybe_unused]] Chunk & chunk)
{
    // TODO reall need to clone the chunk??
    switch (type)
    {
        case Type::Out:
            query_cache_writer->buffer(chunk.clone());
            break;
        case Type::Totals:
            query_cache_writer->bufferTotals(chunk.clone());
            break;
        case Type::Extremes:
            query_cache_writer->bufferExtremes(chunk.clone());
            break;

    }
}

void StreamInQueryCacheTransform::finalizeWriteInQueryCache()
{
    if (!isCancelled())
        query_cache_writer->finalizeWrite();
}

};
