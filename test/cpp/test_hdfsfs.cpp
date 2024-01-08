#include "gtest/gtest.h"

TEST(HDFSTest, BasicAssertions) {
  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  std::string abc = "hhello";
  EXPECT_EQ(7 * 6, 42);
}