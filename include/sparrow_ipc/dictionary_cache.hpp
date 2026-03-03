#pragma once

#include <cstdint>
#include <map>
#include <optional>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Caches dictionaries during deserialization.
     *
     * This class stores dictionaries received from DictionaryBatch messages and
     * provides them when reconstructing dictionary-encoded arrays. Delta updates
     * append dictionary values to existing dictionaries with the same ID.
     *
     * Dictionaries are stored as single-column record batches and are referenced
     * by their integer ID. Multiple fields can share the same dictionary by
     * referencing the same ID.
     */
    class SPARROW_IPC_API dictionary_cache
    {
    public:
        /**
         * @brief Store or update a dictionary.
         *
         * Stores or updates a dictionary identified by the given ID.
         * If is_delta is true and a dictionary with the same ID already exists,
         * values are appended to the existing dictionary.
         *
         * @param id The dictionary ID
         * @param batch The dictionary data as a single-column record batch
         * @param is_delta If true, append values to existing dictionary with the same ID
         * @throws std::invalid_argument if batch doesn't have exactly one column
         */
        void store_dictionary(int64_t id, sparrow::record_batch batch, bool is_delta);

        /**
         * @brief Retrieve a cached dictionary.
         *
         * @param id The dictionary ID to retrieve
         * @return An optional containing the dictionary if found, std::nullopt otherwise
         */
        std::optional<std::reference_wrapper<const sparrow::record_batch>> get_dictionary(int64_t id) const;

        /**
         * @brief Check if a dictionary is cached.
         *
         * @param id The dictionary ID to check
         * @return true if the dictionary exists in the cache, false otherwise
         */
        [[nodiscard]] bool contains(int64_t id) const noexcept;

        /**
         * @brief Clear all cached dictionaries.
         */
        void clear() noexcept;

        /**
         * @brief Get the number of cached dictionaries.
         *
         * @return The number of dictionaries in the cache
         */
        [[nodiscard]] size_t size() const noexcept ;

    private:
        std::map<int64_t, sparrow::record_batch> m_dictionaries;
    };
}
