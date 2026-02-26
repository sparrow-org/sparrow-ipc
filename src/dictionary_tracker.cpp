// Copyright 2024 Man Group Operations Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sparrow_ipc/dictionary_tracker.hpp"

#include "sparrow_ipc/dictionary_utils.hpp"

#include <algorithm>
#include <string>
#include <unordered_set>

#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/arrow_interface/arrow_array_schema_utils.hpp>

namespace sparrow_ipc
{
    namespace
    {
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
            const ArrowSchema* child_schema = parent_schema.children[child_index];
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
            std::unordered_set<int64_t>& extracted_dict_ids,
            std::vector<dictionary_info>& dictionaries
        )
        {
            if (has_dictionary(&schema))
            {
                const int64_t dict_id = extract_dictionary_id(&schema, fallback_name, fallback_index);
                if (!emitted_dict_ids.contains(dict_id) && !extracted_dict_ids.contains(dict_id))
                {
                    ArrowSchema* dict_schema = schema.dictionary;
                    if (array.dictionary == nullptr)
                    {
                        throw std::runtime_error(
                            "ArrowArray must have dictionary data when ArrowSchema has dictionary"
                        );
                    }

                    std::vector<sparrow::array> dict_arrays;
                    dict_arrays.reserve(1);
                    dict_arrays.emplace_back(array.dictionary, dict_schema);

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
                }

                if (schema.dictionary != nullptr && array.dictionary != nullptr)
                {
                    extract_dictionaries_from_array_recursive(
                        *array.dictionary,
                        *schema.dictionary,
                        fallback_name,
                        0,
                        emitted_dict_ids,
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
            for (size_t child_index = 0; child_index < child_count; ++child_index)
            {
                const ArrowSchema* child_schema = schema.children[child_index];
                const ArrowArray* child_array = array.children[child_index];
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

        // Scan each column for dictionary encoding
        for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx)
        {
            const auto& column = columns[column_idx];
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            const auto& schema = arrow_proxy.schema();
            const auto& array = arrow_proxy.array();
            const std::string_view fallback_name = column_idx < names.size()
                                                       ? std::string_view(names[column_idx])
                                                       : std::string_view("__dictionary__");

            extract_dictionaries_from_array_recursive(
                array,
                schema,
                fallback_name,
                column_idx,
                m_emitted_dict_ids,
                extracted_dict_ids,
                dictionaries
            );
        }

        return dictionaries;
    }

    void dictionary_tracker::mark_emitted(int64_t id)
    {
        m_emitted_dict_ids.insert(id);
    }

    bool dictionary_tracker::is_emitted(int64_t id) const
    {
        return m_emitted_dict_ids.contains(id);
    }

    void dictionary_tracker::reset()
    {
        m_emitted_dict_ids.clear();
    }
}
