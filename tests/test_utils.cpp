#include <doctest/doctest.h>

#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    TEST_CASE("align_to_8")
    {
        CHECK_EQ(utils::align_to_8(0), 0);
        CHECK_EQ(utils::align_to_8(1), 8);
        CHECK_EQ(utils::align_to_8(7), 8);
        CHECK_EQ(utils::align_to_8(8), 8);
        CHECK_EQ(utils::align_to_8(9), 16);
        CHECK_EQ(utils::align_to_8(15), 16);
        CHECK_EQ(utils::align_to_8(16), 16);
    }

    TEST_CASE("get_substr_after_separator")
    {
        SUBCASE("Basic case")
        {
            auto result = utils::get_substr_after_separator("w:abc", ":");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), "abc");
        }

        SUBCASE("Basic case with number")
        {
            auto result = utils::get_substr_after_separator("w:123", ":");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), "123");
        }

        SUBCASE("Multiple separators")
        {
            auto result = utils::get_substr_after_separator("w:1:23", ":");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), "1:23");
        }

        SUBCASE("Wrong separator")
        {
            auto result = utils::get_substr_after_separator("w:123", ",");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Empty")
        {
            auto result = utils::get_substr_after_separator("", ":");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Nothing after existing separator")
        {
            auto result = utils::get_substr_after_separator("w:", ":");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), "");
        }

        SUBCASE("Without separator")
        {
            auto result = utils::get_substr_after_separator("abc", ":");
            CHECK_FALSE(result.has_value());
        }
    }

    TEST_CASE("get_substr_as_int32")
    {
        SUBCASE("Basic case")
        {
            auto result = utils::get_substr_as_int32("w:123", ":");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 123);
        }

        SUBCASE("Basic case with str")
        {
            auto result = utils::get_substr_as_int32("w:abc", ":");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Multiple separators")
        {
            auto result = utils::get_substr_as_int32("w:1:23", ":");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Wrong separator")
        {
            auto result = utils::get_substr_as_int32("w:123", ",");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Empty")
        {
            auto result = utils::get_substr_as_int32("", ":");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Nothing after existing separator")
        {
            auto result = utils::get_substr_as_int32("w:", ":");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Without separator")
        {
            auto result = utils::get_substr_as_int32("123", ":");
            CHECK_FALSE(result.has_value());
        }
    }

    TEST_CASE("extract_words_after_colon")
    {
        SUBCASE("Basic case with multiple words")
        {
            auto result = utils::extract_words_after_colon("d:128,10");
            REQUIRE_EQ(result.size(), 2);
            CHECK_EQ(result[0], "128");
            CHECK_EQ(result[1], "10");
        }

        SUBCASE("Single word after colon")
        {
            auto result = utils::extract_words_after_colon("w:256");
            REQUIRE_EQ(result.size(), 1);
            CHECK_EQ(result[0], "256");
        }

        SUBCASE("Three words")
        {
            auto result = utils::extract_words_after_colon("d:10,5,128");
            REQUIRE_EQ(result.size(), 3);
            CHECK_EQ(result[0], "10");
            CHECK_EQ(result[1], "5");
            CHECK_EQ(result[2], "128");
        }

        SUBCASE("No colon in string")
        {
            auto result = utils::extract_words_after_colon("no_colon");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Colon at end")
        {
            auto result = utils::extract_words_after_colon("prefix:");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Empty string")
        {
            auto result = utils::extract_words_after_colon("");
            CHECK_EQ(result.size(), 0);
        }

        SUBCASE("Only colon and comma")
        {
            auto result = utils::extract_words_after_colon(":,");
            REQUIRE_EQ(result.size(), 2);
            CHECK_EQ(result[0], "");
            CHECK_EQ(result[1], "");
        }

        SUBCASE("Complex prefix")
        {
            auto result = utils::extract_words_after_colon("prefix:word1,word2,word3");
            REQUIRE_EQ(result.size(), 3);
            CHECK_EQ(result[0], "word1");
            CHECK_EQ(result[1], "word2");
            CHECK_EQ(result[2], "word3");
        }
    }

    TEST_CASE("parse_to_int32")
    {
        SUBCASE("Valid positive integer")
        {
            auto result = utils::parse_to_int32("123");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 123);
        }

        SUBCASE("Valid negative integer")
        {
            auto result = utils::parse_to_int32("-456");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), -456);
        }

        SUBCASE("Zero")
        {
            auto result = utils::parse_to_int32("0");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 0);
        }

        SUBCASE("Large valid number")
        {
            auto result = utils::parse_to_int32("2147483647");  // INT32_MAX
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 2147483647);
        }

        SUBCASE("Invalid - not a number")
        {
            auto result = utils::parse_to_int32("abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - empty string")
        {
            auto result = utils::parse_to_int32("");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - partial number with text")
        {
            auto result = utils::parse_to_int32("123abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - text with number")
        {
            auto result = utils::parse_to_int32("abc123");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - just a sign")
        {
            auto result = utils::parse_to_int32("-");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Valid with leading zeros")
        {
            auto result = utils::parse_to_int32("00123");
            REQUIRE(result.has_value());
            CHECK_EQ(result.value(), 123);
        }
    }

    TEST_CASE("parse_decimal_format")
    {
        SUBCASE("Basic format: d:19,10")
        {
            auto result = utils::parse_decimal_format("d:19,10");
            REQUIRE(result.has_value());
            const auto& [precision, scale, bitwidth] = result.value();
            CHECK_EQ(precision, 19);
            CHECK_EQ(scale, 10);
            CHECK_FALSE(bitwidth.has_value());
        }

        SUBCASE("Extended format: d:19,10,128")
        {
            auto result = utils::parse_decimal_format("d:19,10,128");
            REQUIRE(result.has_value());
            const auto& [precision, scale, bitwidth] = result.value();
            CHECK_EQ(precision, 19);
            CHECK_EQ(scale, 10);
            REQUIRE(bitwidth.has_value());
            CHECK_EQ(bitwidth.value(), 128);
        }

        SUBCASE("Extended format: d:38,6,256")
        {
            auto result = utils::parse_decimal_format("d:38,6,256");
            REQUIRE(result.has_value());
            const auto& [precision, scale, bitwidth] = result.value();
            CHECK_EQ(precision, 38);
            CHECK_EQ(scale, 6);
            REQUIRE(bitwidth.has_value());
            CHECK_EQ(bitwidth.value(), 256);
        }

        SUBCASE("Basic format with zero scale: d:10,0")
        {
            auto result = utils::parse_decimal_format("d:10,0");
            REQUIRE(result.has_value());
            const auto& [precision, scale, bitwidth] = result.value();
            CHECK_EQ(precision, 10);
            CHECK_EQ(scale, 0);
            CHECK_FALSE(bitwidth.has_value());
        }

        SUBCASE("Extended format with zero scale: d:10,0,64")
        {
            auto result = utils::parse_decimal_format("d:10,0,64");
            REQUIRE(result.has_value());
            const auto& [precision, scale, bitwidth] = result.value();
            CHECK_EQ(precision, 10);
            CHECK_EQ(scale, 0);
            REQUIRE(bitwidth.has_value());
            CHECK_EQ(bitwidth.value(), 64);
        }

        SUBCASE("Invalid - no colon")
        {
            auto result = utils::parse_decimal_format("d19,10");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - no comma")
        {
            auto result = utils::parse_decimal_format("d:19");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - missing precision")
        {
            auto result = utils::parse_decimal_format("d:,10");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - missing scale")
        {
            auto result = utils::parse_decimal_format("d:19,");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - missing bitwidth when provided")
        {
            auto result = utils::parse_decimal_format("d:19,10,");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - non-numeric precision")
        {
            auto result = utils::parse_decimal_format("d:abc,10");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - non-numeric scale")
        {
            auto result = utils::parse_decimal_format("d:19,abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - non-numeric bitwidth")
        {
            auto result = utils::parse_decimal_format("d:19,10,abc");
            CHECK_FALSE(result.has_value());
        }

        SUBCASE("Invalid - empty string")
        {
            auto result = utils::parse_decimal_format("");
            CHECK_FALSE(result.has_value());
        }
    }
}
