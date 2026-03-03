#include "sparrow_ipc/dictionary_tracker.hpp"

#include "sparrow_ipc/dictionary_utils.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
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
        struct dictionary_id_result
        {
            int64_t id;
            bool from_metadata;
        };

        dictionary_id_result extract_dictionary_id(
            const ArrowSchema* schema,
            int64_t& next_fallback_dictionary_id
        )
        {
            if (schema == nullptr || schema->dictionary == nullptr)
            {
                throw std::invalid_argument("Schema must have a dictionary");
            }

            const auto metadata = parse_dictionary_metadata(*schema);
            if (metadata.id.has_value())
            {
                return {.id = *metadata.id, .from_metadata = true};
            }

            return {.id = next_fallback_dictionary_id++, .from_metadata = false};
        }

        void validate_dictionary_id_origin(
            int64_t dict_id,
            std::string_view origin,
            bool from_metadata,
            std::unordered_map<int64_t, std::string>& dictionary_id_origins
        )
        {
            if (!from_metadata)
            {
                return;
            }

            const auto [it, inserted] = dictionary_id_origins.try_emplace(dict_id, origin);
            if (!inserted && it->second != origin)
            {
                throw std::runtime_error(
                    "Duplicate dictionary id " + std::to_string(dict_id)
                    + " is used by multiple dictionary fields"
                );
            }
        }

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
            std::string_view field_path,
            int64_t& next_fallback_dictionary_id,
            const std::set<int64_t>& emitted_dict_ids,
            const std::unordered_map<int64_t, std::size_t>& emitted_dict_sizes,
            std::unordered_map<int64_t, std::size_t>& pending_dict_sizes,
            std::unordered_map<int64_t, std::string>& dictionary_id_origins,
            std::unordered_set<int64_t>& extracted_dict_ids,
            std::vector<dictionary_info>& dictionaries
        )
        {
            if (has_dictionary(&schema))
            {
                const auto dictionary_id = extract_dictionary_id(&schema, next_fallback_dictionary_id);
                const int64_t dict_id = dictionary_id.id;
                validate_dictionary_id_origin(
                    dict_id,
                    field_path,
                    dictionary_id.from_metadata,
                    dictionary_id_origins
                );

                ArrowSchema* dict_schema = schema.dictionary;
                if (array.dictionary == nullptr)
                {
                    throw std::runtime_error(
                        "ArrowArray must have dictionary data when ArrowSchema has dictionary"
                    );
                }

                auto dictionary_values = sparrow::array(array.dictionary, dict_schema);
                const std::size_t dictionary_size = dictionary_values.size();

                bool should_emit = !extracted_dict_ids.contains(dict_id)
                                   && !emitted_dict_ids.contains(dict_id);
                if (!should_emit && emitted_dict_ids.contains(dict_id) && !extracted_dict_ids.contains(dict_id))
                {
                    if (const auto emitted_it = emitted_dict_sizes.find(dict_id);
                        emitted_it != emitted_dict_sizes.end() && emitted_it->second != dictionary_size)
                    {
                        throw std::runtime_error(
                            "Dictionary id " + std::to_string(dict_id)
                            + " was already emitted with size " + std::to_string(emitted_it->second)
                            + " but now has size " + std::to_string(dictionary_size)
                            + ". Use delta dictionary updates for dictionary growth."
                        );
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
                            .is_delta = false
                        }
                    );
                    extracted_dict_ids.insert(dict_id);
                    pending_dict_sizes.insert_or_assign(dict_id, dictionary_size);
                }

                if (schema.dictionary != nullptr && array.dictionary != nullptr)
                {
                    const std::string dictionary_path = std::string(field_path) + "/dictionary";
                    extract_dictionaries_from_array_recursive(
                        *array.dictionary,
                        *schema.dictionary,
                        dictionary_path,
                        next_fallback_dictionary_id,
                        emitted_dict_ids,
                        emitted_dict_sizes,
                        pending_dict_sizes,
                        dictionary_id_origins,
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
                const std::string child_path = std::string(field_path)
                                               + "/"
                                               + std::to_string(child_index)
                                               + ":"
                                               + child_name;
                extract_dictionaries_from_array_recursive(
                    *child_array,
                    *child_schema,
                    child_path,
                    next_fallback_dictionary_id,
                    emitted_dict_ids,
                    emitted_dict_sizes,
                    pending_dict_sizes,
                    dictionary_id_origins,
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
        int64_t next_fallback_dictionary_id = 0;
        m_pending_dict_sizes.clear();

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
            const std::string field_path = std::string(fallback_name);

            extract_dictionaries_from_array_recursive(
                array,
                schema,
                field_path,
                next_fallback_dictionary_id,
                m_emitted_dict_ids,
                m_emitted_dict_sizes,
                m_pending_dict_sizes,
                m_dictionary_id_origins,
                extracted_dict_ids,
                dictionaries
            );
        }

        return dictionaries;
    }

    void dictionary_tracker::mark_emitted(int64_t id) noexcept
    {
        m_emitted_dict_ids.insert(id);
        if (const auto pending_it = m_pending_dict_sizes.find(id);
            pending_it != m_pending_dict_sizes.end())
        {
            m_emitted_dict_sizes.insert_or_assign(id, pending_it->second);
            m_pending_dict_sizes.erase(pending_it);
        }
    }

    bool dictionary_tracker::is_emitted(int64_t id) const noexcept
    {
        return m_emitted_dict_ids.contains(id);
    }

    void dictionary_tracker::reset() noexcept
    {
        m_emitted_dict_ids.clear();
        m_dictionary_id_origins.clear();
        m_emitted_dict_sizes.clear();
        m_pending_dict_sizes.clear();
    }
}
