//
//  test_default_field_inline.cpp
//
//  Bug: DEFAULT_FIELD was declared as `const std::string` in a header
//  without `inline`, creating one copy per translation unit (ODR violation).
//
//  Fix: Changed to `inline const std::string`.
//
//  This test verifies that DEFAULT_FIELD is accessible and consistent
//  across translation units (this is a separate TU from the helpers tests).
//

#include <gtest/gtest.h>
#include "RAL_Helpers.hpp"

// Address of DEFAULT_FIELD as seen from this TU
const std::string* default_field_addr_this_tu() { return &DEFAULT_FIELD; }

// Declared in test_ral_helpers.cpp (different TU)
extern const std::string* default_field_addr_other_tu();

TEST(DefaultFieldInline, ValueIsUnderscore)
{
  EXPECT_EQ(DEFAULT_FIELD, "_");
}

TEST(DefaultFieldInline, SameAddressAcrossTranslationUnits)
{
  // With `inline`, both TUs should see the same object.
  // Without `inline`, each TU would have its own copy.
  EXPECT_EQ(default_field_addr_this_tu(), default_field_addr_other_tu());
}
