#include "sparrow_ipc/dictionary_cache.hpp"

#include <algorithm>
#include <iterator>
#include <ranges>
#include <stdexcept>
#include <string>
#include <vector>

#include <sparrow/layout/array_access.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/utils/nullable.hpp>
#include <sparrow/variable_size_binary_array.hpp>
#include <sparrow/arrow_interface/arrow_array_schema_utils.hpp>

namespace sparrow_ipc
{
    namespace
    {
        template <typename T>
        sparrow::array concatenate_primitive_arrays(const sparrow::array& lhs, const sparrow::array& rhs)
        {
            const auto lhs_proxy = sparrow::detail::array_access::get_arrow_proxy(lhs);
            const auto rhs_proxy = sparrow::detail::array_access::get_arrow_proxy(rhs);

            const auto lhs_typed = sparrow::primitive_array<T>(lhs_proxy);
            const auto rhs_typed = sparrow::primitive_array<T>(rhs_proxy);

            auto merged = sparrow::primitive_array<T>(lhs_typed);
            merged.insert(merged.cend(), rhs_typed.cbegin(), rhs_typed.cend());
            return sparrow::array(std::move(merged));
        }

        template <typename StringArray>
        sparrow::array concatenate_string_like_arrays(const sparrow::array& lhs, const sparrow::array& rhs)
        {
            const auto lhs_proxy = sparrow::detail::array_access::get_arrow_proxy(lhs);
            const auto rhs_proxy = sparrow::detail::array_access::get_arrow_proxy(rhs);

            const auto lhs_typed = StringArray(lhs_proxy);
            const auto rhs_typed = StringArray(rhs_proxy);

            std::vector<sparrow::nullable<std::string>> merged_values;
            merged_values.reserve(lhs_typed.size() + rhs_typed.size());

            const auto to_nullable_string = [](const auto& value) -> sparrow::nullable<std::string>
            {
                if (value.has_value())
                {
                    return sparrow::nullable<std::string>(std::string(value.value()));
                }
                return sparrow::nullable<std::string>();
            };

            std::ranges::copy(
                lhs_typed | std::views::transform(to_nullable_string),
                std::back_inserter(merged_values)
            );
            std::ranges::copy(
                rhs_typed | std::views::transform(to_nullable_string),
                std::back_inserter(merged_values)
            );

            auto merged = StringArray(merged_values);
            return sparrow::array(std::move(merged));
        }

        sparrow::array concatenate_dictionary_arrays(const sparrow::array& lhs, const sparrow::array& rhs)
        {
            if (lhs.data_type() != rhs.data_type())
            {
                throw std::runtime_error("Delta dictionary update has mismatched dictionary value types");
            }

            switch (lhs.data_type())
            {
                case sparrow::data_type::BOOL:
                    return concatenate_primitive_arrays<bool>(lhs, rhs);
                case sparrow::data_type::UINT8:
                    return concatenate_primitive_arrays<uint8_t>(lhs, rhs);
                case sparrow::data_type::INT8:
                    return concatenate_primitive_arrays<int8_t>(lhs, rhs);
                case sparrow::data_type::UINT16:
                    return concatenate_primitive_arrays<uint16_t>(lhs, rhs);
                case sparrow::data_type::INT16:
                    return concatenate_primitive_arrays<int16_t>(lhs, rhs);
                case sparrow::data_type::UINT32:
                    return concatenate_primitive_arrays<uint32_t>(lhs, rhs);
                case sparrow::data_type::INT32:
                    return concatenate_primitive_arrays<int32_t>(lhs, rhs);
                case sparrow::data_type::UINT64:
                    return concatenate_primitive_arrays<uint64_t>(lhs, rhs);
                case sparrow::data_type::INT64:
                    return concatenate_primitive_arrays<int64_t>(lhs, rhs);
                case sparrow::data_type::FLOAT:
                    return concatenate_primitive_arrays<float>(lhs, rhs);
                case sparrow::data_type::DOUBLE:
                    return concatenate_primitive_arrays<double>(lhs, rhs);
                case sparrow::data_type::STRING:
                    return concatenate_string_like_arrays<sparrow::string_array>(lhs, rhs);
                case sparrow::data_type::LARGE_STRING:
                    return concatenate_string_like_arrays<sparrow::big_string_array>(lhs, rhs);
                // TODO: add BINARY and LARGE_BINARY support - requires a byte-vector accumulator
                // analogous to concatenate_string_like_arrays but using std::vector<uint8_t> elements.
                default:
                    throw std::runtime_error(
                        "Delta dictionary update is not supported for this dictionary value type. "
                        "Supported types: bool, [u]int{8,16,32,64}, float, double, string, large_string."
                    );
            }
        }
    }

    void dictionary_cache::store_dictionary(int64_t id, sparrow::record_batch batch, bool is_delta)
    {
        if (batch.nb_columns() != 1)
        {
            throw std::invalid_argument("Dictionary batch must have exactly one column");
        }

        if (is_delta)
        {
            auto it = m_dictionaries.find(id);
            if (it != m_dictionaries.end())
            {
                const auto& existing_batch = it->second;
                if (existing_batch.nb_columns() != 1)
                {
                    throw std::runtime_error("Stored dictionary must have exactly one column");
                }

                const sparrow::array merged_values = concatenate_dictionary_arrays(
                    existing_batch.get_column(0),
                    batch.get_column(0)
                );

                std::vector<sparrow::array> merged_columns;
                merged_columns.emplace_back(merged_values);
                it->second = sparrow::record_batch(existing_batch.names(), std::move(merged_columns));
                return;
            }
        }

        m_dictionaries.insert_or_assign(id, std::move(batch));
    }

    std::optional<std::reference_wrapper<const sparrow::record_batch>>
    dictionary_cache::get_dictionary(int64_t id) const
    {
        auto it = m_dictionaries.find(id);
        if (it != m_dictionaries.end())
        {
            return std::cref(it->second);
        }
        return std::nullopt;
    }

    bool dictionary_cache::contains(int64_t id) const noexcept
    {
        return m_dictionaries.contains(id);
    }

    void dictionary_cache::clear() noexcept
    {
        m_dictionaries.clear();
    }

    size_t dictionary_cache::size() const noexcept
    {
        return m_dictionaries.size();
    }
}
