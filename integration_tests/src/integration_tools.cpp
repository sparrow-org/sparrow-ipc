#include "integration_tools.hpp"

#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include "sparrow_ipc/stream_file_serializer.hpp"

#if defined(__cpp_lib_format)
#    include <format>
#endif

#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>

#include <sparrow/json_reader/json_parser.hpp>

namespace integration_tools
{
    nlohmann::json parse_json_file(const std::filesystem::path& json_path)
    {
        if (!std::filesystem::exists(json_path))
        {
            throw std::runtime_error("JSON file not found: " + json_path.string());
        }

        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            throw std::runtime_error("Could not open JSON file: " + json_path.string());
        }

        nlohmann::json json_data;
        try
        {
            json_data = nlohmann::json::parse(json_file);
        }
        catch (const nlohmann::json::parse_error& e)
        {
            throw std::runtime_error("Failed to parse JSON file: " + std::string(e.what()));
        }
        json_file.close();

        return json_data;
    }

    std::vector<uint8_t> json_file_to_arrow_file(const std::filesystem::path& json_path)
    {
        auto json_data = parse_json_file(json_path);
        // Build a record batch with zero length to get the schema, which is
        // useful for cases with zero record batches in the "batches" array.
        const auto schema_batch = sparrow::json_reader::build_record_batch_from_json(json_data, 0);

        const std::vector<uint8_t> stream_data = json_to_stream(json_data);
        return stream_to_file(std::span<const uint8_t>(stream_data), schema_batch);
    }

    std::vector<uint8_t> json_file_to_stream(const std::filesystem::path& json_path)
    {
        auto json_data = parse_json_file(json_path);

        return json_to_stream(json_data);
    }

    std::vector<uint8_t> json_to_stream(const nlohmann::json& json_data)
    {
        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            throw std::runtime_error("JSON file does not contain a 'batches' array");
        }

        const size_t num_batches = json_data["batches"].size();

        std::vector<sparrow::record_batch> record_batches;
        record_batches.reserve(num_batches);

        if (num_batches == 0)
        {
            // If there are no batches, we still need the schema.
            // sparrow::json_reader::build_record_batch_from_json with num_batches=0
            // will create an empty record batch with the correct schema if "batches" is empty.
            try
            {
                record_batches.emplace_back(
                    sparrow::json_reader::build_record_batch_from_json(json_data, 0)
                );
            }
            catch (const std::exception& e)
            {
                 throw std::runtime_error("Failed to build schema from JSON: " + std::string(e.what()));
            }
        }
        else
        {
            for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
            {
                try
                {
                    record_batches.emplace_back(
                        sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                    );
                }
                catch (const std::exception& e)
                {
                    throw std::runtime_error(
                        "Failed to build record batch " + std::to_string(batch_idx) + ": " + e.what()
                    );
                }
            }
        }

        // TODO refactor and move around code here to avoid handling multiple  if conditions
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream stream(stream_data);
        sparrow_ipc::any_output_stream any_stream(stream);
        // sparrow_ipc::serializer serializer(stream);

        if (num_batches == 0)
        {
            // We write the empty batch to establish the schema in the stream
            // serializer << record_batches[0] << sparrow_ipc::end_stream;

            // If there are no batches, we just write the schema and end the stream.
            // This ensures pyarrow (and others) see zero batches but know the schema
            sparrow_ipc::serialize_schema_message(record_batches[0], any_stream);
            stream.write(sparrow_ipc::end_of_stream);
        }
        else
        {
            sparrow_ipc::serializer serializer(stream);
            serializer << record_batches << sparrow_ipc::end_stream;
        }

        return stream_data;
    }

    std::vector<uint8_t> stream_to_file(
        std::span<const uint8_t> input_stream_data,
        std::optional<sparrow::record_batch> schema_batch
    )
    {
        if (input_stream_data.empty())
        {
            throw std::runtime_error("Input stream data is empty");
        }

        sparrow_ipc::record_batch_stream stream_content;
        try
        {
            stream_content = sparrow_ipc::deserialize_stream_to_record_batches(input_stream_data);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error("Failed to deserialize stream: " + std::string(e.what()));
        }

        std::vector<uint8_t> output_stream_data;
        sparrow_ipc::memory_output_stream stream(output_stream_data);

        // Determine which schema to use: the one from the stream,
        // or the explicitly provided schema_batch.
        sparrow::record_batch final_schema_batch = schema_batch.has_value()
                                                    ? schema_batch.value()
                                                    : stream_content.schema;

        if (final_schema_batch.nb_columns() == 0 && stream_content.schema.nb_columns() == 0)
        {
             // If we still have no schema and no batches, we cannot proceed for a file
             // (unless we allow empty schema files, but Arrow usually requires fields)
             if (stream_content.batches.empty())
             {
                 throw std::runtime_error("Cannot create Arrow file: no schema available and no record batches in stream.");
             }
             final_schema_batch = stream_content.batches[0];
        }

        sparrow_ipc::stream_file_serializer serializer(stream, final_schema_batch);

        if (!stream_content.batches.empty())
        {
            serializer << stream_content.batches;
        }

        serializer.end();

        return output_stream_data;
    }

    std::vector<uint8_t> file_to_stream(std::span<const uint8_t> input_file_data)
    {
        if (input_file_data.empty())
        {
            throw std::runtime_error("Input file data is empty");
        }

        std::vector<sparrow::record_batch> record_batches;
        try
        {
            record_batches = sparrow_ipc::deserialize_file(input_file_data);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error("Failed to deserialize file: " + std::string(e.what()));
        }

        std::vector<uint8_t> output_stream_data;
        sparrow_ipc::memory_output_stream stream(output_stream_data);
        sparrow_ipc::serializer serializer(stream);
        serializer << record_batches << sparrow_ipc::end_stream;

        return output_stream_data;
    }

    bool compare_record_batch(
        const sparrow::record_batch& rb1,
        const sparrow::record_batch& rb2,
        size_t batch_idx,
        bool verbose
    )
    {
        bool all_match = true;

        if (rb1.nb_columns() != rb2.nb_columns())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of columns: "
                          << rb1.nb_columns() << " vs " << rb2.nb_columns() << "\n";
            }
            return false;
        }

        if (rb1.nb_rows() != rb2.nb_rows())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of rows: " << rb1.nb_rows()
                          << " vs " << rb2.nb_rows() << "\n";
            }
            return false;
        }

        const auto& names1 = rb1.names();
        const auto& names2 = rb2.names();
        if (names1.size() != names2.size())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of column names\n";
            }
            all_match = false;
        }
        else
        {
            for (size_t i = 0; i < names1.size(); ++i)
            {
                if (names1[i] != names2[i])
                {
                    if (verbose)
                    {
                        std::cerr << "Error: Batch " << batch_idx << " column " << i
                                  << " has different name: '" << names1[i] << "' vs '" << names2[i] << "'\n";
                    }
                    all_match = false;
                }
            }
        }

        for (size_t col_idx = 0; col_idx < rb1.nb_columns(); ++col_idx)
        {
            const auto& col1 = rb1.get_column(col_idx);
            const auto& col2 = rb2.get_column(col_idx);

            if (col1.size() != col2.size())
            {
                if (verbose)
                {
                    std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx
                              << " has different size: " << col1.size() << " vs " << col2.size() << "\n";
                }
                all_match = false;
                continue;
            }

            if (col1.data_type() != col2.data_type())
            {
                if (verbose)
                {
                    std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx
                              << " has different data type\n";
                }
                all_match = false;
                continue;
            }

            const auto col_name1 = col1.name();
            const auto col_name2 = col2.name();
            if (col_name1 != col_name2)
            {
                if (verbose)
                {
                    std::cerr << "Warning: Batch " << batch_idx << ", column " << col_idx
                              << " has different name in column metadata\n";
                }
            }

            for (size_t row_idx = 0; row_idx < col1.size(); ++row_idx)
            {
                if (col1[row_idx] != col2[row_idx])
                {
                    if (verbose)
                    {
                        std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx << " ('"
                                  << col_name1.value_or("unnamed") << "'), row " << row_idx
                                  << " has different value\n";
#if defined(__cpp_lib_format)
                        std::cerr << "  JSON value:   " << std::format("{}", col1[row_idx]) << "\n";
                        std::cerr << "  Stream value: " << std::format("{}", col2[row_idx]) << "\n";
#endif
                    }
                    all_match = false;
                }
            }
        }

        return all_match;
    }

    bool validate_json_against_arrow_file(
        const std::filesystem::path& json_path,
        std::span<const uint8_t> arrow_file_data
    )
    {
        auto json_data = parse_json_file(json_path);

        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            throw std::runtime_error("JSON file does not contain a 'batches' array");
        }

        const size_t num_batches = json_data["batches"].size();

        std::vector<sparrow::record_batch> json_batches;
        json_batches.reserve(num_batches);

        for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
        {
            try
            {
                json_batches.emplace_back(
                    sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                );
            }
            catch (const std::exception& e)
            {
                throw std::runtime_error(
                    "Failed to build record batch " + std::to_string(batch_idx) + " from JSON: " + e.what()
                );
            }
        }

        if (arrow_file_data.empty())
        {
            throw std::runtime_error("Arrow file data is empty");
        }

        std::vector<sparrow::record_batch> stream_batches;
        try
        {
            stream_batches = sparrow_ipc::deserialize_file(arrow_file_data);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error("Failed to deserialize Arrow file: " + std::string(e.what()));
        }

        if (json_batches.size() != stream_batches.size())
        {
            return false;
        }

        for (size_t batch_idx = 0; batch_idx < json_batches.size(); ++batch_idx)
        {
            if (!compare_record_batch(json_batches[batch_idx], stream_batches[batch_idx], batch_idx, false))
            {
                return false;
            }
        }

        return true;
    }
}
