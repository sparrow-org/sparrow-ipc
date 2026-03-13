#include <vector>
#include <cstring>

#include <doctest/doctest.h>

#include <sparrow/buffer/buffer.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"

namespace sparrow_ipc
{
    TEST_SUITE("C Data Interface")
    {
        TEST_CASE("ArrowArray")
        {
            SUBCASE("make_arrow_array with owned buffers")
            {
                std::vector<uint8_t> buffer1 = {1, 2, 3, 4};
                std::vector<uint8_t> buffer2 = {5, 6, 7, 8};

                std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
                buffers.emplace_back(sparrow::buffer<uint8_t>(buffer1.begin(), buffer1.end(), sparrow::buffer<uint8_t>::default_allocator()));
                buffers.emplace_back(sparrow::buffer<uint8_t>(buffer2.begin(), buffer2.end(), sparrow::buffer<uint8_t>::default_allocator()));

                ArrowArray array = make_arrow_array<arrow_array_private_data>(
                    4,  // length
                    0,  // null_count
                    0,  // offset
                    0,  // children_count
                    nullptr, // children
                    nullptr, // dictionary
                    std::move(buffers)
                );

                CHECK_EQ(array.length, 4);
                CHECK_EQ(array.null_count, 0);
                CHECK_EQ(array.offset, 0);
                CHECK_EQ(array.n_buffers, 2);
                CHECK_EQ(array.n_children, 0);
                CHECK_EQ(array.children, nullptr);
                CHECK_EQ(array.dictionary, nullptr);
                REQUIRE_NE(array.buffers, nullptr);
                REQUIRE_NE(array.private_data, nullptr);

                const auto** ptrs = array.buffers;
                CHECK_EQ(std::memcmp(ptrs[0], buffer1.data(), 4), 0);
                CHECK_EQ(std::memcmp(ptrs[1], buffer2.data(), 4), 0);

                CHECK_EQ(array.release, &arrow_array_release<arrow_array_private_data>);

                array.release(&array);
                CHECK_EQ(array.private_data, nullptr);
                CHECK_EQ(array.buffers, nullptr);
                CHECK_EQ(array.release, nullptr);
            }

            SUBCASE("make_arrow_array with children and dictionary")
            {
                ArrowArray** children = new ArrowArray*[1];
                children[0] = new ArrowArray();
                *children[0] = make_arrow_array<arrow_array_private_data>(5, 1, 0, 0, nullptr, nullptr, std::vector<arrow_array_private_data::optionally_owned_buffer>{});

                ArrowArray* dictionary = new ArrowArray();
                *dictionary = make_arrow_array<arrow_array_private_data>(10, 0, 2, 0, nullptr, nullptr, std::vector<arrow_array_private_data::optionally_owned_buffer>{});

                ArrowArray array = make_arrow_array<arrow_array_private_data>(
                    10, 2, 1, 1, children, dictionary, std::vector<arrow_array_private_data::optionally_owned_buffer>{}
                );

                CHECK_EQ(array.length, 10);
                CHECK_EQ(array.null_count, 2);
                CHECK_EQ(array.offset, 1);
                CHECK_EQ(array.n_children, 1);
                CHECK_EQ(array.children, children);
                CHECK_EQ(array.dictionary, dictionary);

                CHECK_EQ(array.children[0]->length, 5);
                CHECK_EQ(array.children[0]->null_count, 1);
                CHECK_EQ(array.children[0]->offset, 0);
                CHECK_EQ(array.children[0]->n_children, 0);

                CHECK_EQ(array.dictionary->length, 10);
                CHECK_EQ(array.dictionary->null_count, 0);
                CHECK_EQ(array.dictionary->offset, 2);
                CHECK_EQ(array.dictionary->n_children, 0);

                CHECK_EQ(array.buffers, nullptr);

                CHECK_EQ(array.release, &arrow_array_release<arrow_array_private_data>);

                array.release(&array);
                CHECK_EQ(array.private_data, nullptr);
                CHECK_EQ(array.release, nullptr);
                // Children and dictionary should be released by `arrow_array_release`
                // calling `release_arrow_array_children_and_dictionary`
                CHECK_EQ(array.children, nullptr);
                CHECK_EQ(array.dictionary, nullptr);
            }
        }
    }
}
