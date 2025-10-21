/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/collection/immutable_btree.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using lsm::collection::immutable_btree;

class ImmutableBTreeTest : public testing::Test {
public:
    // Helper to collect all elements from an iterator into a vector
    template<typename Key, typename Value>
    static std::vector<std::pair<Key, Value>>
    collect_iterator(immutable_btree<Key, Value> tree) {
        return collect_iterator_coro(std::move(tree)).get();
    }

    template<typename Key, typename Value>
    static ss::future<std::vector<std::pair<Key, Value>>>
    collect_iterator_coro(immutable_btree<Key, Value> tree) {
        std::vector<std::pair<Key, Value>> result;
        auto gen = tree.iterator();
        while (auto item = co_await gen()) {
            result.push_back(*item);
        }
        co_return result;
    }
};

TEST_F(ImmutableBTreeTest, EmptyTree) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    EXPECT_EQ(tree.empty(), reference.empty());
    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(1), std::nullopt);
}

TEST_F(ImmutableBTreeTest, SingleInsert) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(1, "one");
    reference[1] = "one";

    EXPECT_EQ(tree.empty(), reference.empty());
    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(1), "one");
    EXPECT_EQ(tree.get(2), std::nullopt);
}

TEST_F(ImmutableBTreeTest, MultipleInserts) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.insert(3, "three");
    reference[3] = "three";
    tree = tree.insert(7, "seven");
    reference[7] = "seven";
    tree = tree.insert(1, "one");
    reference[1] = "one";
    tree = tree.insert(9, "nine");
    reference[9] = "nine";

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(1), "one");
    EXPECT_EQ(tree.get(3), "three");
    EXPECT_EQ(tree.get(5), "five");
    EXPECT_EQ(tree.get(7), "seven");
    EXPECT_EQ(tree.get(9), "nine");
    EXPECT_EQ(tree.get(2), std::nullopt);
}

TEST_F(ImmutableBTreeTest, UpdateExistingKey) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.insert(5, "FIVE");
    reference[5] = "FIVE";

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(5), "FIVE");
}

TEST_F(ImmutableBTreeTest, RemoveSingleElement) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.remove(5);
    reference.erase(5);

    EXPECT_EQ(tree.empty(), reference.empty());
    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(5), std::nullopt);
}

TEST_F(ImmutableBTreeTest, RemoveNonExistentElement) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.remove(10);
    reference.erase(10); // No-op for map

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(5), "five");
}

TEST_F(ImmutableBTreeTest, RemoveLeafNode) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.insert(3, "three");
    reference[3] = "three";
    tree = tree.insert(7, "seven");
    reference[7] = "seven";

    // Remove a leaf node
    tree = tree.remove(3);
    reference.erase(3);

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(3), std::nullopt);
    EXPECT_EQ(tree.get(5), "five");
    EXPECT_EQ(tree.get(7), "seven");
}

TEST_F(ImmutableBTreeTest, RemoveNodeWithOneChild) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.insert(3, "three");
    reference[3] = "three";
    tree = tree.insert(1, "one");
    reference[1] = "one";

    // Remove node with one child
    tree = tree.remove(3);
    reference.erase(3);

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(1), "one");
    EXPECT_EQ(tree.get(3), std::nullopt);
    EXPECT_EQ(tree.get(5), "five");
}

TEST_F(ImmutableBTreeTest, RemoveNodeWithTwoChildren) {
    immutable_btree<int, std::string> tree;
    std::map<int, std::string> reference;

    tree = tree.insert(5, "five");
    reference[5] = "five";
    tree = tree.insert(3, "three");
    reference[3] = "three";
    tree = tree.insert(7, "seven");
    reference[7] = "seven";
    tree = tree.insert(1, "one");
    reference[1] = "one";
    tree = tree.insert(4, "four");
    reference[4] = "four";

    // Remove node with two children
    tree = tree.remove(3);
    reference.erase(3);

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get(1), "one");
    EXPECT_EQ(tree.get(3), std::nullopt);
    EXPECT_EQ(tree.get(4), "four");
    EXPECT_EQ(tree.get(5), "five");
    EXPECT_EQ(tree.get(7), "seven");
}

TEST_F(ImmutableBTreeTest, Immutability) {
    immutable_btree<int, std::string> tree1;
    tree1 = tree1.insert(1, "one");
    tree1 = tree1.insert(2, "two");

    // Create a new tree from tree1
    auto tree2 = tree1.insert(3, "three");

    // tree1 should be unchanged
    EXPECT_EQ(tree1.size(), 2u);
    EXPECT_EQ(tree1.get(1), "one");
    EXPECT_EQ(tree1.get(2), "two");
    EXPECT_EQ(tree1.get(3), std::nullopt);

    // tree2 should have all three elements
    EXPECT_EQ(tree2.size(), 3u);
    EXPECT_EQ(tree2.get(1), "one");
    EXPECT_EQ(tree2.get(2), "two");
    EXPECT_EQ(tree2.get(3), "three");

    // Remove from tree2
    auto tree3 = tree2.remove(2);

    // tree2 should be unchanged
    EXPECT_EQ(tree2.size(), 3u);
    EXPECT_EQ(tree2.get(2), "two");

    // tree3 should have the element removed
    EXPECT_EQ(tree3.size(), 2u);
    EXPECT_EQ(tree3.get(1), "one");
    EXPECT_EQ(tree3.get(2), std::nullopt);
    EXPECT_EQ(tree3.get(3), "three");
}

TEST_F(ImmutableBTreeTest, SequentialInserts) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    constexpr int n = 100;
    for (int i = 0; i < n; ++i) {
        tree = tree.insert(i, i * 10);
        reference[i] = i * 10;
    }

    EXPECT_EQ(tree.size(), reference.size());

    for (int i = 0; i < n; ++i) {
        EXPECT_EQ(tree.get(i), i * 10);
    }
}

TEST_F(ImmutableBTreeTest, ReverseSequentialInserts) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    constexpr int n = 100;
    for (int i = n - 1; i >= 0; --i) {
        tree = tree.insert(i, i * 10);
        reference[i] = i * 10;
    }

    EXPECT_EQ(tree.size(), reference.size());

    for (int i = 0; i < n; ++i) {
        EXPECT_EQ(tree.get(i), i * 10);
    }
}

TEST_F(ImmutableBTreeTest, RandomInsertsAndRemoves) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    std::mt19937 gen(12345); // Fixed seed for reproducibility
    std::uniform_int_distribution<> key_dist(1, 1000);
    std::uniform_int_distribution<> value_dist(1, 10000);
    std::uniform_int_distribution<> op_dist(0, 1);

    constexpr int operations = 10000;
    for (int i = 0; i < operations; ++i) {
        int op = op_dist(gen);
        int key = key_dist(gen);
        int value = value_dist(gen);

        if (op == 0 || reference.empty()) {
            // Insert
            tree = tree.insert(key, value);
            reference[key] = value;
        } else {
            // Remove
            tree = tree.remove(key);
            reference.erase(key);
        }

        // Verify size matches
        EXPECT_EQ(tree.size(), reference.size())
          << "Size mismatch at operation " << i;
    }

    // Verify all keys
    for (const auto& [key, value] : reference) {
        EXPECT_EQ(tree.get(key), value) << "Value mismatch for key " << key;
    }
}

TEST_F(ImmutableBTreeTest, StressTestWithRandomOperations) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    std::mt19937 gen(54321); // Fixed seed for reproducibility
    std::uniform_int_distribution<> key_dist(1, 500);
    std::uniform_int_distribution<> value_dist(1, 10000);
    std::uniform_int_distribution<> op_dist(0, 2);

    constexpr int operations = 50000;
    for (int i = 0; i < operations; ++i) {
        int op = op_dist(gen);
        int key = key_dist(gen);
        int value = value_dist(gen);

        if (op == 0) {
            // Insert
            tree = tree.insert(key, value);
            reference[key] = value;
        } else if (op == 1 && !reference.empty()) {
            // Remove
            tree = tree.remove(key);
            reference.erase(key);
        } else {
            // Get (verify)
            auto tree_result = tree.get(key);
            auto ref_it = reference.find(key);
            if (ref_it != reference.end()) {
                EXPECT_EQ(tree_result, ref_it->second)
                  << "Value mismatch for key " << key << " at operation " << i;
            } else {
                EXPECT_EQ(tree_result, std::nullopt)
                  << "Expected nullopt for key " << key << " at operation "
                  << i;
            }
        }
    }

    // Final verification
    EXPECT_EQ(tree.size(), reference.size());
    for (const auto& [key, value] : reference) {
        EXPECT_EQ(tree.get(key), value)
          << "Final value mismatch for key " << key;
    }
}

TEST_F(ImmutableBTreeTest, StringKeys) {
    immutable_btree<std::string, int> tree;
    std::map<std::string, int> reference;

    tree = tree.insert("apple", 1);
    reference["apple"] = 1;
    tree = tree.insert("banana", 2);
    reference["banana"] = 2;
    tree = tree.insert("cherry", 3);
    reference["cherry"] = 3;
    tree = tree.insert("date", 4);
    reference["date"] = 4;

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get("apple"), 1);
    EXPECT_EQ(tree.get("banana"), 2);
    EXPECT_EQ(tree.get("cherry"), 3);
    EXPECT_EQ(tree.get("date"), 4);
    EXPECT_EQ(tree.get("elderberry"), std::nullopt);

    tree = tree.remove("banana");
    reference.erase("banana");

    EXPECT_EQ(tree.size(), reference.size());
    EXPECT_EQ(tree.get("banana"), std::nullopt);
    EXPECT_EQ(tree.get("apple"), 1);
    EXPECT_EQ(tree.get("cherry"), 3);
}

TEST_F(ImmutableBTreeTest, AVLBalancingLeftLeft) {
    // Test Left-Left case rotation
    immutable_btree<int, int> tree;

    tree = tree.insert(30, 30);
    tree = tree.insert(20, 20);
    tree = tree.insert(10, 10); // Should trigger right rotation

    EXPECT_EQ(tree.size(), 3u);
    EXPECT_EQ(tree.get(10), 10);
    EXPECT_EQ(tree.get(20), 20);
    EXPECT_EQ(tree.get(30), 30);
}

TEST_F(ImmutableBTreeTest, AVLBalancingRightRight) {
    // Test Right-Right case rotation
    immutable_btree<int, int> tree;

    tree = tree.insert(10, 10);
    tree = tree.insert(20, 20);
    tree = tree.insert(30, 30); // Should trigger left rotation

    EXPECT_EQ(tree.size(), 3u);
    EXPECT_EQ(tree.get(10), 10);
    EXPECT_EQ(tree.get(20), 20);
    EXPECT_EQ(tree.get(30), 30);
}

TEST_F(ImmutableBTreeTest, AVLBalancingLeftRight) {
    // Test Left-Right case rotation
    immutable_btree<int, int> tree;

    tree = tree.insert(30, 30);
    tree = tree.insert(10, 10);
    tree = tree.insert(20, 20); // Should trigger left-right rotation

    EXPECT_EQ(tree.size(), 3u);
    EXPECT_EQ(tree.get(10), 10);
    EXPECT_EQ(tree.get(20), 20);
    EXPECT_EQ(tree.get(30), 30);
}

TEST_F(ImmutableBTreeTest, AVLBalancingRightLeft) {
    // Test Right-Left case rotation
    immutable_btree<int, int> tree;

    tree = tree.insert(10, 10);
    tree = tree.insert(30, 30);
    tree = tree.insert(20, 20); // Should trigger right-left rotation

    EXPECT_EQ(tree.size(), 3u);
    EXPECT_EQ(tree.get(10), 10);
    EXPECT_EQ(tree.get(20), 20);
    EXPECT_EQ(tree.get(30), 30);
}

TEST_F(ImmutableBTreeTest, ComplexBalancingScenario) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    // Insert in an order that would cause multiple rotations
    std::vector<int> keys = {50, 25, 75, 10, 30, 60, 80, 5, 15, 27, 55, 65};

    for (int key : keys) {
        tree = tree.insert(key, key * 10);
        reference[key] = key * 10;
    }

    EXPECT_EQ(tree.size(), reference.size());

    for (int key : keys) {
        EXPECT_EQ(tree.get(key), key * 10);
    }

    // Now remove some nodes to test balancing during removal
    std::vector<int> keys_to_remove = {10, 30, 60};
    for (int key : keys_to_remove) {
        tree = tree.remove(key);
        reference.erase(key);
    }

    EXPECT_EQ(tree.size(), reference.size());

    for (const auto& [key, value] : reference) {
        EXPECT_EQ(tree.get(key), value);
    }
}

TEST_F(ImmutableBTreeTest, LargeDataset) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    constexpr int n = 100000;
    std::mt19937 gen(99999);
    std::vector<int> keys(n);
    std::iota(keys.begin(), keys.end(), 0);
    std::shuffle(keys.begin(), keys.end(), gen);

    // Insert in random order
    for (int key : keys) {
        tree = tree.insert(key, key * 10);
        reference[key] = key * 10;
    }

    EXPECT_EQ(tree.size(), reference.size());

    // Verify all keys
    for (int key : keys) {
        EXPECT_EQ(tree.get(key), key * 10);
    }

    // Remove half the keys
    std::shuffle(keys.begin(), keys.end(), gen);
    for (size_t i = 0; i < keys.size() / 2; ++i) {
        tree = tree.remove(keys[i]);
        reference.erase(keys[i]);
    }

    EXPECT_EQ(tree.size(), reference.size());

    // Verify remaining keys
    for (const auto& [key, value] : reference) {
        EXPECT_EQ(tree.get(key), value);
    }
}

// Iterator tests

TEST_F(ImmutableBTreeTest, IteratorEmptyTree) {
    immutable_btree<int, int> tree;
    auto elements = collect_iterator(tree);
    EXPECT_TRUE(elements.empty());
}

TEST_F(ImmutableBTreeTest, IteratorSingleElement) {
    immutable_btree<int, std::string> tree;
    tree = tree.insert(42, "answer");

    auto elements = collect_iterator(tree);
    ASSERT_EQ(elements.size(), 1u);
    EXPECT_EQ(elements[0].first, 42);
    EXPECT_EQ(elements[0].second, "answer");
}

TEST_F(ImmutableBTreeTest, IteratorMultipleElements) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    // Insert in random order
    std::vector<int> keys = {5, 2, 8, 1, 3, 7, 9};
    for (int key : keys) {
        tree = tree.insert(key, key * 10);
        reference[key] = key * 10;
    }

    // Iterator should return elements in sorted order
    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<int, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first);
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second);
    }
}

TEST_F(ImmutableBTreeTest, IteratorAfterRemove) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    // Build initial tree
    for (int i = 1; i <= 10; ++i) {
        tree = tree.insert(i, i * 10);
        reference[i] = i * 10;
    }

    // Remove some elements
    tree = tree.remove(3);
    reference.erase(3);
    tree = tree.remove(7);
    reference.erase(7);

    // Verify iterator matches reference
    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<int, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first)
          << "Mismatch at index " << i;
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second)
          << "Mismatch at index " << i;
    }
}

TEST_F(ImmutableBTreeTest, IteratorImmutability) {
    immutable_btree<int, int> tree1;
    tree1 = tree1.insert(1, 10);
    tree1 = tree1.insert(2, 20);
    tree1 = tree1.insert(3, 30);

    // Create a new tree with additional element
    auto tree2 = tree1.insert(4, 40);

    // Iterator on tree1 should only see original 3 elements
    auto tree1_elements = collect_iterator(tree1);
    ASSERT_EQ(tree1_elements.size(), 3u);
    EXPECT_EQ(tree1_elements[0], std::make_pair(1, 10));
    EXPECT_EQ(tree1_elements[1], std::make_pair(2, 20));
    EXPECT_EQ(tree1_elements[2], std::make_pair(3, 30));

    // Iterator on tree2 should see all 4 elements
    auto tree2_elements = collect_iterator(tree2);
    ASSERT_EQ(tree2_elements.size(), 4u);
    EXPECT_EQ(tree2_elements[0], std::make_pair(1, 10));
    EXPECT_EQ(tree2_elements[1], std::make_pair(2, 20));
    EXPECT_EQ(tree2_elements[2], std::make_pair(3, 30));
    EXPECT_EQ(tree2_elements[3], std::make_pair(4, 40));

    // Remove from tree2
    auto tree3 = tree2.remove(2);

    // tree2 iterator should still see all 4 elements
    tree2_elements = collect_iterator(tree2);
    ASSERT_EQ(tree2_elements.size(), 4u);

    // tree3 iterator should see 3 elements (without key 2)
    auto tree3_elements = collect_iterator(tree3);
    ASSERT_EQ(tree3_elements.size(), 3u);
    EXPECT_EQ(tree3_elements[0], std::make_pair(1, 10));
    EXPECT_EQ(tree3_elements[1], std::make_pair(3, 30));
    EXPECT_EQ(tree3_elements[2], std::make_pair(4, 40));
}

TEST_F(ImmutableBTreeTest, IteratorStringKeys) {
    immutable_btree<std::string, int> tree;
    std::map<std::string, int> reference;

    tree = tree.insert("dog", 1);
    reference["dog"] = 1;
    tree = tree.insert("apple", 2);
    reference["apple"] = 2;
    tree = tree.insert("zebra", 3);
    reference["zebra"] = 3;
    tree = tree.insert("banana", 4);
    reference["banana"] = 4;

    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<std::string, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first);
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second);
    }
}

TEST_F(ImmutableBTreeTest, IteratorSequentialInserts) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    constexpr int n = 100;
    for (int i = 0; i < n; ++i) {
        tree = tree.insert(i, i * 10);
        reference[i] = i * 10;
    }

    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<int, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first);
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second);
    }
}

TEST_F(ImmutableBTreeTest, IteratorRandomOperations) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    std::mt19937 gen(54321);
    std::uniform_int_distribution<> key_dist(1, 100);
    std::uniform_int_distribution<> value_dist(1, 1000);
    std::uniform_int_distribution<> op_dist(0, 1);

    // Perform random operations
    constexpr int operations = 5000;
    for (int i = 0; i < operations; ++i) {
        int op = op_dist(gen);
        int key = key_dist(gen);
        int value = value_dist(gen);

        if (op == 0 || reference.empty()) {
            tree = tree.insert(key, value);
            reference[key] = value;
        } else {
            tree = tree.remove(key);
            reference.erase(key);
        }
    }

    // Verify iterator produces same sequence as std::map
    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<int, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first)
          << "Key mismatch at index " << i;
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second)
          << "Value mismatch at index " << i;
    }
}

TEST_F(ImmutableBTreeTest, IteratorLargeDataset) {
    immutable_btree<int, int> tree;
    std::map<int, int> reference;

    constexpr int n = 1000;
    std::mt19937 gen(99999);
    std::vector<int> keys(n);
    std::iota(keys.begin(), keys.end(), 0);
    std::shuffle(keys.begin(), keys.end(), gen);

    // Insert in random order
    for (int key : keys) {
        tree = tree.insert(key, key * 10);
        reference[key] = key * 10;
    }

    // Verify iterator produces all elements in sorted order
    auto tree_elements = collect_iterator(tree);
    std::vector<std::pair<int, int>> ref_elements(
      reference.begin(), reference.end());

    ASSERT_EQ(tree_elements.size(), ref_elements.size());
    for (size_t i = 0; i < tree_elements.size(); ++i) {
        EXPECT_EQ(tree_elements[i].first, ref_elements[i].first);
        EXPECT_EQ(tree_elements[i].second, ref_elements[i].second);
    }
}
