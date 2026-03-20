#include "sparrow_ipc/stream_file_serializer.hpp"

#include <ranges>

#include <File_generated.h>

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"

namespace sparrow_ipc
{
    stream_file_serializer::~stream_file_serializer()
    {
        if (!m_ended && m_schema_received)
        {
            // Only end if we've written something
            // Don't throw from destructor
            try
            {
                end();
            }
            catch (...)
            {
                // Swallow exceptions in destructor
            }
        }
    }

    void stream_file_serializer::write(const sparrow::record_batch& rb)
    {
        write(std::ranges::single_view(rb));
    }

    void stream_file_serializer::end()
    {
        if (m_ended)
        {
            return;
        }

        if (!m_schema_received)
        {
            throw std::runtime_error("Cannot end file serializer without a schema");
        }

        // If header hasn't been written (e.g. default constructor and no record batches written), write it now
        if (!m_header_written)
        {
            m_stream.write(arrow_file_header_magic);
            m_stream.add_padding();
            m_header_written = true;
        }

        // Write end-of-stream marker
        m_stream.write(end_of_stream);

        // Write footer using the first record batch for schema and the tracked blocks
        const size_t footer_size = write_footer(
            m_first_record_batch.value(),
            m_dictionary_blocks,
            m_record_batch_blocks,
            m_stream
        );

        // Write footer size (int32, little-endian)
        const int32_t footer_size_i32 = static_cast<int32_t>(footer_size);
        m_stream.write(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&footer_size_i32), sizeof(int32_t))
        );

        // Write magic bytes at the end
        m_stream.write(arrow_file_magic);

        m_ended = true;
    }

    size_t write_footer(
        const sparrow::record_batch& record_batch,
        const std::vector<record_batch_block>& dictionary_blocks,
        const std::vector<record_batch_block>& record_batch_blocks,
        any_output_stream& stream
    )
    {
        // Build footer using FlatBufferBuilder
        flatbuffers::FlatBufferBuilder footer_builder;

        // Create schema for footer
        const auto fields_vec = create_children(footer_builder, record_batch);
        const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(
            footer_builder,
            org::apache::arrow::flatbuf::Endianness::Little, // TODO: make configurable
            fields_vec
        );

        // Create dictionaries vector from tracked blocks
        std::vector<org::apache::arrow::flatbuf::Block> dictionary_fb_blocks;
        dictionary_fb_blocks.reserve(dictionary_blocks.size());
        for (const auto& block : dictionary_blocks)
        {
            dictionary_fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
        }
        auto dictionaries_fb = footer_builder.CreateVectorOfStructs(dictionary_fb_blocks);

        // Create record batches vector from tracked blocks
        std::vector<org::apache::arrow::flatbuf::Block> fb_blocks;
        fb_blocks.reserve(record_batch_blocks.size());
        for (const auto& block : record_batch_blocks)
        {
            fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
        }
        auto record_batches_fb = footer_builder.CreateVectorOfStructs(fb_blocks);

        // Create footer
        auto footer = org::apache::arrow::flatbuf::CreateFooter(
            footer_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            schema_offset,
            dictionaries_fb,
            record_batches_fb
        );

        footer_builder.Finish(footer);

        // Write footer
        const uint8_t* footer_data = footer_builder.GetBufferPointer();
        const flatbuffers::uoffset_t footer_size = footer_builder.GetSize();
        stream.write(std::span<const uint8_t>(footer_data, footer_size));
        return footer_size;
    }

    std::vector<sparrow::record_batch> deserialize_file(std::span<const uint8_t> data)
    {
        // Validate minimum file size
        // Magic (8) + Footer size (4) + Magic (6) = 18 bytes minimum
        constexpr size_t min_file_size = 18;
        if (data.size() < min_file_size)
        {
            throw std::runtime_error("File is too small to be a valid Arrow file");
        }

        // Check magic bytes at the beginning
        if (!is_arrow_file_magic(data.subspan(0, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at start");
        }

        // Check magic bytes at the end
        const size_t trailing_magic_offset = data.size() - arrow_file_magic_size;
        if (!is_arrow_file_magic(data.subspan(trailing_magic_offset, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at end");
        }

        // Read footer size (4 bytes before the trailing magic)
        const size_t footer_size_offset = data.size() - arrow_file_magic_size - sizeof(int32_t);
        int32_t footer_size = 0;
        std::memcpy(&footer_size, data.data() + footer_size_offset, sizeof(int32_t));

        if (footer_size <= 0 || static_cast<size_t>(footer_size) > data.size() - min_file_size)
        {
            throw std::runtime_error("Invalid footer size in Arrow file");
        }

        // Calculate the end of the stream data (before footer)
        const size_t footer_offset = footer_size_offset - footer_size;
        
        // Extract the stream portion (from after header magic to before footer)
        // Stream data starts after the 8-byte header magic
        const size_t stream_start = arrow_file_header_magic.size();
        const size_t stream_length = footer_offset - stream_start;
        
        auto stream_data = data.subspan(stream_start, stream_length);
        
        // Use deserialize_stream to parse the stream format data
        // This handles schema message, record batches, and end-of-stream marker
        return deserialize_stream(stream_data);
    }
}
