constexpr int BASES[] = {2, 3};
constexpr int MAX_DIGITS[] = {63, 40};

template<int BASE, int MAX_DIGITS>
__device__ float get_random_point(const long index, float q[], int d[]) {

    float value = 0.0f;

    long k = index;

    for (int i = 0; i < MAX_DIGITS; i++) {
        q[i] = (i == 0 ? 1.0f : q[i - 1]) / BASE;
        d[i] = (int) (k % BASE);
        k = (k - d[i]) / BASE;
        value += d[i] * q[i];
    }

    for (int i = 0; i < MAX_DIGITS; i++) {
        d[i]++;
        value += q[i];
        if (d[i] < BASE) {
            break;
        }
        value -= (i == 0 ? 1.0f : q[i - 1]);
    }

    return value;
}

extern "C"
__global__ void qmc_mapper(unsigned long long counters[2], const long count, const long offset) {

    const int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < count) {
        const int index_offset = index + offset;

        float q[MAX_DIGITS[0]] = {0.0f};
        int d[MAX_DIGITS[0]] = {0};

        const float x = get_random_point<BASES[0], MAX_DIGITS[0]>(index_offset, q, d) - 0.5f;
        const float y = get_random_point<BASES[1], MAX_DIGITS[1]>(index_offset, q, d) - 0.5f;

        if (x * x + y * y > 0.25f) {
            atomicAdd(&counters[0], 1);
        } else {
            atomicAdd(&counters[1], 1);
        }
    }
}
