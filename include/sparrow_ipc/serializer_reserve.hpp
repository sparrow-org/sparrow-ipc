#pragma once

#include <numeric>
#include <ranges>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/dictionary_iteration.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    [[nodiscard]] std::size_t calculate_serializer_reserve_size(
        const R& record_batches,
        std::size_t current_stream_size,
        bool schema_received,
        std::optional<CompressionType> compression,
        const dictionary_tracker& dict_tracker,
        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt
    )
    {
        if (std::ranges::empty(record_batches))
        {
            return current_stream_size;
        }

        dictionary_tracker reservation_tracker = dict_tracker;
        const std::size_t total_with_record_batches = std::accumulate(
            record_batches.begin(),
            record_batches.end(),
            current_stream_size,
            [&cache, &reservation_tracker, compression](std::size_t acc, const sparrow::record_batch& rb)
            {
                std::size_t dictionaries_size = 0;
                for_each_pending_dictionary(rb, reservation_tracker, [&](const dictionary_info& dict_info)
                {
                    dictionaries_size += calculate_dictionary_batch_message_size(
                        dict_info.id,
                        dict_info.data,
                        dict_info.is_delta,
                        compression,
                        cache
                    );
                });

                return acc + dictionaries_size
                       + calculate_record_batch_message_size(rb, compression, cache);
            }
        );

        if (!schema_received)
        {
            return total_with_record_batches
                   + calculate_schema_message_size(*record_batches.begin());
        }

        return total_with_record_batches;
    }
}
