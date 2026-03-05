#pragma once

#include <iterator>
#include <ranges>
#include <span>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/deserialize.hpp"

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    class deserializer
    {
    public:

        deserializer(R& data)
            : m_data(&data)
        {
        }

        void deserialize(std::span<const uint8_t> data)
        {
            // Insert at the end of m_data container the deserialized record batches
            auto& container = *m_data;
            auto deserialized_batches = sparrow_ipc::deserialize_stream(data);
            container.insert(
                std::end(container),
                std::make_move_iterator(std::begin(deserialized_batches)),
                std::make_move_iterator(std::end(deserialized_batches))
            );
        }

        deserializer& operator<<(std::span<const uint8_t> data)
        {
            deserialize(data);
            return *this;
        }

    private:

        R* m_data;
    };
}
