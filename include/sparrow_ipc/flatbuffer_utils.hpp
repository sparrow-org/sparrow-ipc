#pragma once

#include <ranges>

#include <flatbuffers/flatbuffers.h>
#include <Message_generated.h>

#include <sparrow/c_interface.hpp>
#include <sparrow/record_batch.hpp>

#include "File_generated.h"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    // Creates a Flatbuffers Decimal type from a format string
    // The format string is expected to be in the format "d:precision,scale"
    [[nodiscard]] std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_decimal_type(
        flatbuffers::FlatBufferBuilder& builder,
        std::string_view format_str,
        const int32_t bitWidth
    );

    // Creates a Flatbuffers type from a format string
    // This function maps a sparrow data type to the corresponding Flatbuffers type
    [[nodiscard]] std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);

    /**
     * @brief Creates a FlatBuffers vector of KeyValue pairs from ArrowSchema metadata.
     *
     * This function converts metadata from an ArrowSchema into a FlatBuffers representation
     * suitable for serialization. It processes key-value pairs from the schema's metadata
     * and creates corresponding FlatBuffers KeyValue objects.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffers objects
     * @param arrow_schema The ArrowSchema containing metadata to be serialized
     *
     * @return A FlatBuffers offset to a vector of KeyValue pairs. Returns 0 if the schema
     *         has no metadata (metadata is nullptr).
     *
     * @note The function reserves memory for the vector based on the metadata size for
     *       optimal performance.
     */
    [[nodiscard]] flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
    create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    /**
     * @brief Creates a FlatBuffer Field object from an ArrowSchema.
     *
     * This function converts an ArrowSchema structure into a FlatBuffer Field representation
     * suitable for Apache Arrow IPC serialization. It handles the creation of all necessary
     * components including field name, type information, metadata, children, and nullable flag.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffer objects
     * @param arrow_schema The ArrowSchema structure containing the field definition to convert
     * @param name_override Optional field name to use instead of the name from arrow_schema.
     *                      If provided, this name will be used regardless of arrow_schema.name.
     *                      If not provided, falls back to arrow_schema.name (or empty if null)
     *
     * @return A FlatBuffer offset to the created Field object that can be used in further
     *         FlatBuffer construction operations
     *
     * @note Dictionary encoding is not currently supported (TODO item)
     * @note The function checks the NULLABLE flag from the ArrowSchema flags to determine nullability
     * @note The name_override parameter is useful when serializing record batches where column
     *       names are stored separately from the array schemas
     */
    [[nodiscard]] ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field> create_field(
        flatbuffers::FlatBufferBuilder& builder,
        const ArrowSchema& arrow_schema,
        std::optional<std::string_view> name_override = std::nullopt
    );

    /**
     * @brief Creates a FlatBuffers vector of Field objects from a record batch.
     *
     * This function extracts column information from a record batch and converts each column
     * into a FlatBuffers Field object. It uses both the column's Arrow schema and the record
     * batch's column names to create properly named fields. The resulting fields are collected
     * into a FlatBuffers vector.
     *
     * @param builder Reference to the FlatBuffers builder used for creating the vector
     * @param record_batch The record batch containing columns and their associated names
     *
     * @return FlatBuffers offset to a vector of Field objects, or 0 if the record batch has no columns
     *
     * @note The function reserves space in the children vector based on the column count
     *       for performance optimization
     * @note Each field is created using the column name from record_batch.names() rather than
     *       from the Arrow schema, ensuring consistency with the record batch structure
     * @note This function properly handles the case where Arrow schemas may not have names
     *       by using the record batch's explicit column names via the name_override parameter
     */
    [[nodiscard]] ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const sparrow::record_batch& record_batch);


    /**
     * @brief Creates a FlatBuffers vector of Field objects from an ArrowSchema's children.
     *
     * This function iterates through all children of the given ArrowSchema and converts
     * each child to a FlatBuffers Field object. The resulting fields are collected into
     * a FlatBuffers vector.
     *
     * @param builder Reference to the FlatBufferBuilder used for creating FlatBuffers objects
     * @param arrow_schema The ArrowSchema containing the children to convert
     *
     * @return A FlatBuffers offset to a vector of Field objects, or 0 if no children exist
     *
     * @throws std::invalid_argument If any child pointer in the ArrowSchema is null
     *
     * @note The function reserves space for all children upfront for performance optimization
     * @note Returns 0 (null offset) when the schema has no children, otherwise returns a valid vector offset
     */
    [[nodiscard]] ::flatbuffers::Offset<
        ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema);

    /**
     * @brief Creates a FlatBuffer builder containing a serialized Arrow schema message.
     *
     * This function constructs an Arrow IPC schema message from a record batch by:
     * 1. Creating field definitions from the record batch columns
     * 2. Building a Schema flatbuffer with little-endian byte order
     * 3. Wrapping the schema in a Message with metadata version V5
     * 4. Finalizing the buffer for serialization
     *
     * @param record_batch The source record batch containing column definitions
     * @return flatbuffers::FlatBufferBuilder A completed FlatBuffer containing the schema message,
     *         ready for Arrow IPC serialization
     *
     * @note The schema message has zero body length as it contains only metadata
     * @note Currently uses little-endian byte order (marked as TODO for configurability)
     */
    [[nodiscard]] flatbuffers::FlatBufferBuilder
    get_schema_message_builder(const sparrow::record_batch& record_batch);

    /**
     * @brief Recursively fills a vector of FieldNode objects from an arrow_proxy and its children.
     *
     * This function creates FieldNode objects containing length and null count information
     * from the given arrow_proxy and recursively processes all its children, appending
     * them to the provided nodes vector in depth-first order.
     *
     * @param arrow_proxy The arrow proxy object containing array metadata (length, null_count)
     *                    and potential child arrays
     * @param nodes Reference to a vector that will be populated with FieldNode objects.
     *              Each FieldNode contains the length and null count of the corresponding array.
     *
     * @note The function reserves space in the nodes vector to optimize memory allocation
     *       when processing children arrays.
     * @note The traversal order is depth-first, with parent nodes added before their children.
     */
    void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    );

    /**
     * @brief Creates a vector of Apache Arrow FieldNode objects from a record batch.
     *
     * This function iterates through all columns in the provided record batch and
     * generates corresponding FieldNode flatbuffer objects. Each column's arrow proxy
     * is used to populate the field nodes vector through the fill_fieldnodes function.
     *
     * @param record_batch The sparrow record batch containing columns to process
     * @return std::vector<org::apache::arrow::flatbuf::FieldNode> Vector of FieldNode
     *         objects representing the structure and metadata of each column
     */
    [[nodiscard]] std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch);

    namespace details
    {
        std::size_t get_nb_buffers_to_process(const std::string_view& format, const std::size_t orig_buffers_size);

        template <typename Func>
        void fill_buffers_impl(
            const sparrow::arrow_proxy& arrow_proxy,
            std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
            int64_t& offset,
            Func&& get_buffer_size
        )
        {
            const auto& buffers = arrow_proxy.buffers();
            auto nb_buffers = get_nb_buffers_to_process(arrow_proxy.schema().format, buffers.size());
            std::ranges::for_each(buffers | std::views::take(nb_buffers),
                [&](const auto& buffer)
            {
                int64_t size = get_buffer_size(buffer);
                flatbuf_buffers.emplace_back(offset, size);
                offset += utils::align_to_8(size);
            });

            for (const auto& child : arrow_proxy.children())
            {
                fill_buffers_impl(child, flatbuf_buffers, offset, get_buffer_size);
            }
        }

        template <typename Func>
        std::vector<org::apache::arrow::flatbuf::Buffer>
        get_buffers_impl(const sparrow::record_batch& record_batch, Func&& fill_buffers_func)
        {
            std::vector<org::apache::arrow::flatbuf::Buffer> buffers;
            int64_t offset = 0;
            for (const auto& column : record_batch.columns())
            {
                const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
                fill_buffers_func(arrow_proxy, buffers, offset);
            }
            return buffers;
        }
    }  // namespace details

    /**
     * @brief Recursively fills a vector of FlatBuffer Buffer objects with buffer information from an Arrow
     * proxy.
     *
     * This function traverses an Arrow proxy structure and creates FlatBuffer Buffer entries for each buffer
     * found in the proxy and its children. The buffers are processed in a depth-first manner, first handling
     * the buffers of the current proxy, then recursively processing all child proxies.
     *
     * @param arrow_proxy The Arrow proxy object containing buffers and potential child proxies to process
     * @param flatbuf_buffers Vector of FlatBuffer Buffer objects to be populated with buffer information
     * @param offset Reference to the current byte offset, updated as buffers are processed and aligned to
     * 8-byte boundaries
     *
     * @note The offset is automatically aligned to 8-byte boundaries using utils::align_to_8() for each
     * buffer
     * @note This function modifies both the flatbuf_buffers vector and the offset parameter
     */
    void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    );

    /**
     * @brief Extracts buffer information from a record batch for serialization.
     *
     * This function iterates through all columns in the provided record batch and
     * collects their buffer information into a vector of Arrow FlatBuffer Buffer objects.
     * The buffers are processed sequentially with cumulative offset tracking.
     *
     * @param record_batch The sparrow record batch containing columns to extract buffers from
     * @return std::vector<org::apache::arrow::flatbuf::Buffer> A vector containing all buffer
     *         descriptors from the record batch columns, with properly calculated offsets
     *
     * @note This function relies on the fill_buffers helper function to process individual
     *       column buffers and maintain offset consistency across all buffers.
     */
    [[nodiscard]] std::vector<org::apache::arrow::flatbuf::Buffer>
    get_buffers(const sparrow::record_batch& record_batch);

    /**
     * @brief Recursively populates a vector with compressed buffer metadata from an Arrow proxy.
     *
     * This function traverses the Arrow proxy and its children, compressing each buffer and recording
     * its metadata (offset and size) in the provided vector. The offset is updated to ensure proper
     * alignment for each subsequent buffer.
     *
     * @param arrow_proxy The Arrow proxy containing the buffers to be compressed.
     * @param flatbuf_compressed_buffers A vector to store the resulting compressed buffer metadata.
     * @param offset The current offset in the buffer layout, which will be updated by the function.
     * @param compression_type The compression algorithm to use.
     * @param cache A cache to store compressed buffers and avoid recompression.
     */
    void fill_compressed_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_compressed_buffers,
        int64_t& offset,
        const CompressionType compression_type,
        CompressionCache& cache
    );

    /**
     * @brief Retrieves metadata describing the layout of compressed buffers within a record batch.
     *
     * This function processes a record batch to determine the metadata (offset and size)
     * for each of its buffers, assuming they are compressed using the specified algorithm.
     * This metadata accounts for each compressed buffer being prefixed by its 8-byte
     * uncompressed size and padded to ensure 8-byte alignment.
     *
     * @param record_batch The record batch whose buffers' compressed metadata is to be retrieved.
     * @param compression_type The compression algorithm that would be applied (e.g., LZ4_FRAME, ZSTD).
     * @param cache A cache to store compressed buffers and avoid recompression.
     * @return A vector of FlatBuffer Buffer objects, each describing the offset and
     *         size of a corresponding compressed buffer within a larger message body.
     */
    [[nodiscard]] std::vector<org::apache::arrow::flatbuf::Buffer> get_compressed_buffers(
        const sparrow::record_batch& record_batch,
        const CompressionType compression_type,
        CompressionCache& cache
    );

    /**
     * @brief Calculates the total aligned size in bytes of all buffers in an Arrow array structure.
     *
     * This function recursively computes the total size needed for all buffers
     * in an Arrow array structure, including buffers from child arrays. Each
     * buffer size is aligned to 8-byte boundaries as required by the Arrow format.
     *
     * @param arrow_proxy The Arrow array proxy containing buffers and child arrays.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache to store and retrieve compressed buffer sizes, avoiding recompression.
     * If compression is given, cache should be set as well.
     * @return int64_t The total aligned size in bytes of all buffers in the array hierarchy.
     * @throws std::invalid_argument if compression is given but not cache.
     */
    [[nodiscard]] int64_t calculate_body_size(
        const sparrow::arrow_proxy& arrow_proxy,
        std::optional<CompressionType> compression = std::nullopt,
        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt
    );

    /**
     * @brief Calculates the total body size of a record batch by summing the body sizes of all its columns.
     *
     * This function iterates through all columns in the given record batch and accumulates
     * the body size of each column's underlying Arrow array proxy. The body size represents
     * the total memory required for the serialized data content of the record batch.
     *
     * @param record_batch The sparrow record batch containing columns to calculate size for.
     * @param compression Optional: The compression type to use when serializing. If not provided, sizes are
     * for uncompressed buffers.
     * @param cache Optional: A cache to store and retrieve compressed buffer sizes, avoiding recompression.
     * If compression is given, cache should be set as well.
     * @return int64_t The total body size in bytes of all columns in the record batch.
     */
    [[nodiscard]] int64_t calculate_body_size(
        const sparrow::record_batch& record_batch,
        std::optional<CompressionType> compression = std::nullopt,
        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt
    );

    /**
     * @brief Creates a FlatBuffer message containing a serialized Apache Arrow RecordBatch.
     *
     * This function builds a complete Arrow IPC message by serializing a record batch
     * along with its metadata (field nodes and buffer information) into a FlatBuffer
     * format that conforms to the Arrow IPC specification.
     *
     * @param record_batch The source record batch containing the data to be serialized.
     * @param compression Optional: The compression algorithm to be used for the message body.
     * @param cache Optional: A cache for compressed buffers to avoid recompression if compression is enabled.
     * If compression is given, cache should be set as well.
     * @return A FlatBufferBuilder containing the complete serialized message ready for
     *         transmission or storage. The builder is finished and ready to be accessed
     *         via GetBufferPointer() and GetSize().
     * @throws std::invalid_argument if compression is given but not cache.
     *
     * @note The returned message uses Arrow IPC format version V5.
     * @note Variadic buffer counts is not currently implemented (set to 0).
     */
    [[nodiscard]] flatbuffers::FlatBufferBuilder get_record_batch_message_builder(
        const sparrow::record_batch& record_batch,
        std::optional<CompressionType> compression = std::nullopt,
        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt
    );

    // Helper function to extract and parse the footer from Arrow IPC file data
    [[nodiscard]] SPARROW_IPC_API const org::apache::arrow::flatbuf::Footer* get_footer_from_file_data(std::span<const uint8_t> file_data);
}
