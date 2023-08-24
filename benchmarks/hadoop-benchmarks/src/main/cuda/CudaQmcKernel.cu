constexpr int BASES[] = {2, 3};
constexpr int MAX_DIGITS[] = {63, 40};

__device__ float get_random_point(const long index, float q[], int d[], int base, int max_digits) {

    float value = 0.0f;

    long k = index;

    for (int i = 0; i < max_digits; i++) {
        q[i] = (i == 0 ? 1.0f : q[i - 1]) / base;
        d[i] = (int) (k % base);
        k = (k - d[i]) / base;
        value += d[i] * q[i];
    }

    for (int i = 0; i < max_digits; i++) {
        d[i]++;
        value += q[i];
        if (d[i] < base) {
            break;
        }
        value -= (i == 0 ? 1.0f : q[i - 1]);
    }

    return value;
}

extern "C"
__global__ void qmc_mapper(short guesses[], const int64_t count, const int64_t offset) {

    const int32_t index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < count) {
        const int64_t index_offset = index + offset;

        float q[64] = {0.0f};
        int d[64] = {0};

        const float x = get_random_point(index_offset, q, d, BASES[0], MAX_DIGITS[0]) - 0.5f;
        const float y = get_random_point(index_offset, q, d, BASES[1], MAX_DIGITS[1]) - 0.5f;

        guesses[index] = (x * x + y * y > 0.25f) ? 1 : 0;
    }
}
