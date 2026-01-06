#pragma once

#include <functional>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <sparrow/array.hpp>

#include "Message_generated.h"

#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    /**
     * @brief A class for deserializing Arrow arrays from Flatbuffers messages.
     *
     * This class uses a map to deserialize different types of Arrow
     * arrays based on the field type specified in the schema. It is designed to be
     * extensible, allowing new deserializers to be added by registering them in the map.
     */
    class array_deserializer
    {
    public:
        /**
         * @brief A function pointer type for the deserializer function.
         *
         * This defines the signature for all functions that can deserialize a specific
         * Arrow array type.
         */
        using deserializer_func = std::function<sparrow::array(
            const org::apache::arrow::flatbuf::RecordBatch&,
            const std::span<const uint8_t>&,
            const std::string&,
            const std::optional<std::vector<sparrow::metadata_pair>>&,
            bool,
            size_t&,
            const org::apache::arrow::flatbuf::Field&
        )>;

        /**
         * @brief Constructs the array_deserializer and initializes the deserializer map.
         *
         * The constructor populates the map with function pointers to the static
         * deserialization methods for each supported Arrow data type.
         */
        array_deserializer();

        /**
         * @brief Deserializes an array based on its field description.
         *
         * This is the main entry point of the deserializer. It looks up the appropriate
         * deserialization function from the map based on the field's type and invokes it.
         *
         * @param record_batch The Flatbuffer RecordBatch containing the data.
         * @param body The raw byte buffer of the message body.
         * @param name The name of the field.
         * @param metadata The metadata associated with the field.
         * @param nullable Whether the field is nullable.
         * @param buffer_index The current index into the buffer list of the RecordBatch.
         * @param field The Flatbuffer Field object describing the array to deserialize.
         * @return A `sparrow::array` containing the deserialized data.
         * @throws std::runtime_error if the field type is not supported.
         */
        sparrow::array deserialize(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                   const std::span<const uint8_t>& body,
                                   const std::string& name,
                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                   bool nullable,
                                   size_t& buffer_index,
                                   const org::apache::arrow::flatbuf::Field& field) const;
    private:
        std::unordered_map<org::apache::arrow::flatbuf::Type, deserializer_func> m_deserializer_map;

        template<typename T>
        static sparrow::array deserialize_primitive(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                    const std::span<const uint8_t>& body,
                                                    const std::string& name,
                                                    const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                    bool nullable,
                                                    size_t& buffer_index,
                                                    const org::apache::arrow::flatbuf::Field&)
        {
            return sparrow::array(deserialize_primitive_array<T>(
                record_batch, body, name, metadata, nullable, buffer_index
            ));
        }

        template<typename T>
        static sparrow::array deserialize_variable_size_binary(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                               const std::span<const uint8_t>& body,
                                                               const std::string& name,
                                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                               bool nullable,
                                                               size_t& buffer_index,
                                                               const org::apache::arrow::flatbuf::Field&)
        {
            return sparrow::array(deserialize_variable_size_binary_array<T>(
                record_batch, body, name, metadata, nullable, buffer_index
            ));
        }

        static sparrow::array deserialize_int(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                              const std::span<const uint8_t>& body,
                                              const std::string& name,
                                              const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                              bool nullable,
                                              size_t& buffer_index,
                                              const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_float(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                const std::span<const uint8_t>& body,
                                                const std::string& name,
                                                const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                bool nullable,
                                                size_t& buffer_index,
                                                const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_fixed_size_binary(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                            const std::span<const uint8_t>& body,
                                                            const std::string& name,
                                                            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                            bool nullable,
                                                            size_t& buffer_index,
                                                            const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_decimal(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                  const std::span<const uint8_t>& body,
                                                  const std::string& name,
                                                  const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                  bool nullable,
                                                  size_t& buffer_index,
                                                  const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_null(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               const org::apache::arrow::flatbuf::Field&);

        static sparrow::array deserialize_date(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_interval(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                   const std::span<const uint8_t>& body,
                                                   const std::string& name,
                                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                   bool nullable,
                                                   size_t& buffer_index,
                                                   const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_duration(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                   const std::span<const uint8_t>& body,
                                                   const std::string& name,
                                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                   bool nullable,
                                                   size_t& buffer_index,
                                                   const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_time(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_timestamp(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                    const std::span<const uint8_t>& body,
                                                    const std::string& name,
                                                    const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                    bool nullable,
                                                    size_t& buffer_index,
                                                    const org::apache::arrow::flatbuf::Field& field);
    };
}
