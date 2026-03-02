#pragma once

#include <functional>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/dictionary_tracker.hpp"

namespace sparrow_ipc
{
    template <class Func>
    void for_each_pending_dictionary(
        const sparrow::record_batch& record_batch,
        dictionary_tracker& tracker,
        Func visitor
    )
    {
        const auto dictionaries = tracker.extract_dictionaries_from_batch(record_batch);
        for (const auto& dict_info : dictionaries)
        {
            std::invoke(visitor, dict_info);
            tracker.mark_emitted(dict_info.id);
        }
    }
}
