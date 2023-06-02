#include "../lib/unity/unity.h"
#include "../src/sbits.h"

void setUp(void) {}

void tearDown(void) {}

void test_sbitsInit_should_init_state(void) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(result, 1, "Error with setup");
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_sbitsInit_should_init_state);
    return UNITY_END();
}
