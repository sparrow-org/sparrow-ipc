#include "sparrow_ipc/utils.hpp"

#include <charconv>

namespace sparrow_ipc::utils
{
    std::optional<std::string_view> get_substr_after_separator(std::string_view str, std::string_view sep)
    {
        const auto sep_pos = str.find(sep);
        if (sep_pos == std::string_view::npos)
        {
            return std::nullopt;
        }
        return str.substr(sep_pos + sep.length());
    }

    std::optional<int32_t> parse_to_int32(std::string_view str)
    {
        int32_t value = 0;
        const auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

        if (ec != std::errc() || ptr != str.data() + str.size())
        {
            return std::nullopt;
        }
        return value;
    }

    std::optional<int32_t> get_substr_as_int32(std::string_view str, std::string_view sep)
    {
        const auto substr_opt = get_substr_after_separator(str, sep);
        if (!substr_opt)
        {
            return std::nullopt;
        }

        const auto& substr_str = substr_opt.value();
        return parse_to_int32(substr_str);
    }

    std::optional<std::tuple<int32_t, int32_t, std::optional<int32_t>>> parse_decimal_format(std::string_view format_str)
    {
        // Format can be "d:precision,scale" or "d:precision,scale,bitWidth"
        // First, find the colon
        const auto colon_pos = format_str.find(':');
        if (colon_pos == std::string_view::npos)
        {
            return std::nullopt;
        }

        // Extract the part after the colon
        std::string_view params_str(format_str.data() + colon_pos + 1, format_str.size() - colon_pos - 1);

        // Find the first comma (between precision and scale)
        const auto first_comma_pos = params_str.find(',');
        if (first_comma_pos == std::string_view::npos)
        {
            return std::nullopt;
        }

        // Parse precision
        std::string_view precision_str(params_str.data(), first_comma_pos);
        auto precision = parse_to_int32(precision_str);
        if (!precision)
        {
            return std::nullopt;
        }

        // Find the second comma (between scale and bitWidth, if present)
        const auto remaining_str = params_str.substr(first_comma_pos + 1);
        const auto second_comma_pos = remaining_str.find(',');

        std::string_view scale_str;
        std::optional<int32_t> bit_width;

        if (second_comma_pos == std::string_view::npos)
        {
            // Format is "d:precision,scale"
            scale_str = remaining_str;
        }
        else
        {
            // Format is "d:precision,scale,bitWidth"
            scale_str = std::string_view(remaining_str.data(), second_comma_pos);
            std::string_view bitwidth_str(remaining_str.data() + second_comma_pos + 1,
                                         remaining_str.size() - second_comma_pos - 1);

            // Parse bitWidth
            auto bw = parse_to_int32(bitwidth_str);
            if (!bw)
            {
                return std::nullopt;
            }
            bit_width = *bw;
        }

        // Parse scale
        auto scale = parse_to_int32(scale_str);
        if (!scale)
        {
            return std::nullopt;
        }

        return std::make_tuple(*precision, *scale, bit_width);
    }

    std::vector<std::string_view> extract_words_after_colon(std::string_view str)
    {
        std::vector<std::string_view> result;

        // Find the position of ':'
        const auto colon_pos = str.find(':');
        if (colon_pos == std::string_view::npos)
        {
            return result;  // Return empty vector if ':' not found
        }

        // Get the substring after ':'
        std::string_view remaining = str.substr(colon_pos + 1);

        // If nothing after ':', return empty vector
        if (remaining.empty())
        {
            return result;
        }

        // Split by ','
        size_t start = 0;
        size_t comma_pos = remaining.find(',');

        while (comma_pos != std::string_view::npos)
        {
            result.push_back(remaining.substr(start, comma_pos - start));
            start = comma_pos + 1;
            comma_pos = remaining.find(',', start);
        }

        // Add the last word (or the only word if no comma was found)
        result.push_back(remaining.substr(start));

        return result;
    }
}
