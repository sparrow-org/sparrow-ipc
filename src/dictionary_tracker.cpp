#include "sparrow_ipc/dictionary_tracker.hpp"

#include "sparrow_ipc/dictionary_utils.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/arrow_interface/arrow_array_schema_utils.hpp>
#include <sparrow/layout/array_access.hpp>

namespace sparrow_ipc
{
    namespace
    {
        constexpr std::size_t fnv_offset_basis = 1469598103934665603ULL;
        constexpr std::size_t fnv_prime = 1099511628211ULL;
        constexpr std::size_t hash_combine_magic = 0x9e3779b97f4a7c15ULL;
        constexpr unsigned hash_combine_left_shift = 6U;
        constexpr unsigned hash_combine_right_shift = 2U;

        std::size_t hash_bytes(std::span<const uint8_t> bytes)
        {
            std::size_t hash = fnv_offset_basis;
            for (uint8_t byte : bytes)
            {
                hash ^= static_cast<std::size_t>(byte);
                hash *= fnv_prime;
            }
            return hash;
        }

        void hash_combine(std::size_t& seed, std::size_t value)
        {
            seed ^= value + hash_combine_magic
                    + (seed << hash_combine_left_shift)
                    + (seed >> hash_combine_right_shift);
        }

        std::size_t hash_schema_shape(const ArrowSchema& schema)
        {
            std::size_t seed = fnv_offset_basis;

            if (schema.format != nullptr)
            {
                hash_combine(seed, std::hash<std::string_view>{}(schema.format));
            }
            if (schema.name != nullptr)
            {
                hash_combine(seed, std::hash<std::string_view>{}(schema.name));
            }
            hash_combine(seed, static_cast<std::size_t>(schema.flags));
            hash_combine(seed, static_cast<std::size_t>(schema.n_children));

            return seed;
        }

        std::size_t hash_array_content_recursive(const sparrow::arrow_proxy& proxy)
        {
            std::size_t seed = hash_schema_shape(proxy.schema());

            hash_combine(seed, static_cast<std::size_t>(proxy.length()));
            hash_combine(seed, static_cast<std::size_t>(proxy.null_count()));
            hash_combine(seed, static_cast<std::size_t>(proxy.offset()));
            hash_combine(seed, static_cast<std::size_t>(proxy.n_buffers()));
            hash_combine(seed, static_cast<std::size_t>(proxy.n_children()));

            for (const auto& buffer : proxy.buffers())
            {
                hash_combine(seed, buffer.size());
                if (!buffer.empty())
                {
                    hash_combine(seed, hash_bytes(buffer));
                }
            }

            for (const auto& child : proxy.children())
            {
                hash_combine(seed, hash_array_content_recursive(child));
            }

            return seed;
        }

        std::size_t compute_dictionary_fingerprint(const sparrow::array& dictionary_values)
        {
            const auto& dictionary_proxy = sparrow::detail::array_access::get_arrow_proxy(dictionary_values);
            return hash_array_content_recursive(dictionary_proxy);
        }

        /**
         * @brief Extract dictionary ID from ArrowSchema metadata.
         *
         * The dictionary ID can be stored in the schema's metadata with key "ARROW:dictionary:id".
         * If not found, we generate an ID from a stable hash of the field name.
         *
         * @param schema The ArrowSchema to extract ID from
         * @return The dictionary ID
         */
        int64_t extract_dictionary_id(
            const ArrowSchema* schema,
            std::string_view fallback_name,
            size_t fallback_index
        )
        {
            if (schema == nullptr || schema->dictionary == nullptr)
            {
                throw std::invalid_argument("Schema must have a dictionary");
            }

            // Try to extract from metadata first
            const auto metadata = parse_dictionary_metadata(*schema);
            if (metadata.id.has_value())
            {
                return *metadata.id;
            }

            // Fallback: use stable hash of field name
            return compute_fallback_dictionary_id(fallback_name, fallback_index);
        }

        /**
         * @brief Check if ArrowSchema has dictionary encoding.
         *
         * @param schema The ArrowSchema to check
         * @return true if the schema has dictionary encoding, false otherwise
         */
        bool has_dictionary(const ArrowSchema* schema)
        {
            return schema != nullptr && schema->dictionary != nullptr;
        }

        std::string get_child_field_name(const ArrowSchema& parent_schema, size_t child_index)
        {
            std::span<ArrowSchema* const> child_schemas(
                parent_schema.children,
                static_cast<size_t>(parent_schema.n_children)
            );
            const ArrowSchema* child_schema = child_schemas[child_index];
            if (child_schema != nullptr && child_schema->name != nullptr)
            {
                return std::string(child_schema->name);
            }

            const std::string parent_name = parent_schema.name != nullptr
                                                ? std::string(parent_schema.name)
                                                : std::string("base");
            return parent_name + "_child_" + std::to_string(child_index);
        }

        void extract_dictionaries_from_array_recursive(
            const ArrowArray& array,
            const ArrowSchema& schema,
            std::string_view fallback_name,
            size_t fallback_index,
            const std::set<int64_t>& emitted_dict_ids,
            const std::unordered_map<int64_t, std::size_t>& emitted_dict_fingerprints,
            std::unordered_map<int64_t, std::size_t>& pending_dict_fingerprints,
            std::unordered_set<int64_t>& extracted_dict_ids,
            std::vector<dictionary_info>& dictionaries
        )
        {
            if (has_dictionary(&schema))
            {
                const int64_t dict_id = extract_dictionary_id(&schema, fallback_name, fallback_index);
                ArrowSchema* dict_schema = schema.dictionary;
                if (array.dictionary == nullptr)
                {
                    throw std::runtime_error(
                        "ArrowArray must have dictionary data when ArrowSchema has dictionary"
                    );
                }

                auto dictionary_values = sparrow::array(array.dictionary, dict_schema);
                const std::size_t dictionary_fingerprint = compute_dictionary_fingerprint(dictionary_values);

                bool should_emit = !extracted_dict_ids.contains(dict_id);
                bool is_delta = false;
                if (should_emit)
                {
                    const bool emitted_before = emitted_dict_ids.contains(dict_id);
                    if (emitted_before)
                    {
                        const auto fingerprint_it = emitted_dict_fingerprints.find(dict_id);
                        if (fingerprint_it != emitted_dict_fingerprints.end())
                        {
                            should_emit = fingerprint_it->second != dictionary_fingerprint;
                            is_delta = false;
                        }
                        else
                        {
                            should_emit = false;
                        }
                    }
                }

                if (should_emit)
                {

                    std::vector<sparrow::array> dict_arrays;
                    dict_arrays.reserve(1);
                    dict_arrays.emplace_back(std::move(dictionary_values));

                    const std::string dict_name = (dict_schema->name != nullptr
                                                   && std::string_view(dict_schema->name).size() > 0)
                                                      ? std::string(dict_schema->name)
                                                      : std::string("__dictionary__");
                    std::vector<std::string> dict_names;
                    dict_names.reserve(1);
                    dict_names.push_back(dict_name);

                    const bool is_ordered = parse_dictionary_metadata(schema).is_ordered;
                    dictionaries.emplace_back(
                        dictionary_info{
                            .id = dict_id,
                            .data = sparrow::record_batch(std::move(dict_names), std::move(dict_arrays)),
                            .is_ordered = is_ordered,
                            .is_delta = is_delta
                        }
                    );
                    extracted_dict_ids.insert(dict_id);
                    pending_dict_fingerprints.insert_or_assign(dict_id, dictionary_fingerprint);
                }

                if (schema.dictionary != nullptr && array.dictionary != nullptr)
                {
                    extract_dictionaries_from_array_recursive(
                        *array.dictionary,
                        *schema.dictionary,
                        fallback_name,
                        0,
                        emitted_dict_ids,
                        emitted_dict_fingerprints,
                        pending_dict_fingerprints,
                        extracted_dict_ids,
                        dictionaries
                    );
                }
            }

            if (schema.n_children <= 0 || schema.children == nullptr || array.children == nullptr)
            {
                return;
            }

            const size_t child_count = std::min(
                static_cast<size_t>(schema.n_children),
                static_cast<size_t>(array.n_children)
            );
            std::span<ArrowSchema* const> child_schemas(schema.children, child_count);
            std::span<ArrowArray* const> child_arrays(array.children, child_count);
            for (size_t child_index = 0; child_index < child_count; ++child_index)
            {
                const ArrowSchema* child_schema = child_schemas[child_index];
                const ArrowArray* child_array = child_arrays[child_index];
                if (child_schema == nullptr || child_array == nullptr)
                {
                    continue;
                }

                const std::string child_name = get_child_field_name(schema, child_index);
                extract_dictionaries_from_array_recursive(
                    *child_array,
                    *child_schema,
                    child_name,
                    child_index,
                    emitted_dict_ids,
                    emitted_dict_fingerprints,
                    pending_dict_fingerprints,
                    extracted_dict_ids,
                    dictionaries
                );
            }
        }

    }

    std::vector<dictionary_info> dictionary_tracker::extract_dictionaries_from_batch(
        const sparrow::record_batch& batch
    )
    {
        std::vector<dictionary_info> dictionaries;
        const auto& columns = batch.columns();
        const auto& names = batch.names();
        std::unordered_set<int64_t> extracted_dict_ids;
        m_pending_dict_fingerprints.clear();

        // Scan each column for dictionary encoding
        for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx)
        {
            const auto& column = columns[column_idx];
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            const auto& schema = arrow_proxy.schema();
            const auto& array = arrow_proxy.array();
            const std::string_view fallback_name = [&names, column_idx]() -> std::string_view
            {
                if (column_idx >= names.size())
                {
                    return "__dictionary__";
                }

                using names_difference_type = std::ranges::range_difference_t<decltype(names)>;
                const auto name_it = std::ranges::next(
                    names.begin(),
                    static_cast<names_difference_type>(column_idx)
                );
                return std::string_view(*name_it);
            }();

            extract_dictionaries_from_array_recursive(
                array,
                schema,
                fallback_name,
                column_idx,
                m_emitted_dict_ids,
                m_emitted_dict_fingerprints,
                m_pending_dict_fingerprints,
                extracted_dict_ids,
                dictionaries
            );
        }

        return dictionaries;
    }

    void dictionary_tracker::mark_emitted(int64_t id) noexcept
    {
        m_emitted_dict_ids.insert(id);
        if (const auto pending_it = m_pending_dict_fingerprints.find(id);
            pending_it != m_pending_dict_fingerprints.end())
        {
            m_emitted_dict_fingerprints.insert_or_assign(id, pending_it->second);
            m_pending_dict_fingerprints.erase(pending_it);
        }
    }

    bool dictionary_tracker::is_emitted(int64_t id) const noexcept
    {
        return m_emitted_dict_ids.contains(id);
    }

    void dictionary_tracker::reset() noexcept
    {
        m_emitted_dict_ids.clear();
        m_emitted_dict_fingerprints.clear();
        m_pending_dict_fingerprints.clear();
    }
}
