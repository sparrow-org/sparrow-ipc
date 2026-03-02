#pragma once

#include <cstdint>
#include <cstddef>
#include <set>
#include <unordered_map>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Information about a dictionary used for encoding.
     */
    struct dictionary_info
    {
        int64_t id;                      ///< Dictionary identifier
        sparrow::record_batch data;      ///< Dictionary values as a single-column record batch
        bool is_ordered;                 ///< Whether dictionary values are ordered
        bool is_delta;                   ///< Whether this is a delta update
    };

    /**
     * @brief Tracks dictionaries during serialization.
     *
     * This class is responsible for discovering dictionary-encoded fields in record batches,
     * extracting their dictionary data, and managing which dictionaries have been emitted
     * to the output stream.
     *
     * Dictionaries must be emitted before any RecordBatch that references them. This class
     * ensures proper ordering by tracking emitted dictionary IDs and providing methods to
     * determine which dictionaries need to be sent before each record batch.
     */
    class SPARROW_IPC_API dictionary_tracker
    {
    public:
        /**
         * @brief Extract dictionaries from a record batch.
         *
         * Scans all columns in the record batch for dictionary-encoded fields.
         * Returns a vector of dictionaries that need to be emitted before this
         * record batch can be serialized.
         *
         * @param batch The record batch to scan for dictionaries
         * @return Vector of dictionary_info for dictionaries that haven't been emitted yet
         */
        std::vector<dictionary_info> extract_dictionaries_from_batch(const sparrow::record_batch& batch);

        /**
         * @brief Mark a dictionary as emitted.
         *
         * After a dictionary has been written to the stream, call this method to
         * record that it has been emitted. This prevents re-emission of the same
         * dictionary for subsequent record batches (unless it's a delta update).
         *
         * @param id The dictionary ID that was emitted
         */
        void mark_emitted(int64_t id);

        /**
         * @brief Check if a dictionary has been emitted.
         *
         * @param id The dictionary ID to check
         * @return true if the dictionary has been emitted, false otherwise
         */
        bool is_emitted(int64_t id) const;

        /**
         * @brief Reset tracking state.
         *
         * Clears all tracking information. Useful when starting a new stream.
         */
        void reset();

    private:
        std::set<int64_t> m_emitted_dict_ids;  ///< IDs of dictionaries already emitted
        std::unordered_map<int64_t, std::size_t> m_emitted_dict_fingerprints;
        std::unordered_map<int64_t, std::size_t> m_pending_dict_fingerprints;
    };
}
