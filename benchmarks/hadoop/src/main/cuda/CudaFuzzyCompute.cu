constexpr int SET_SIZE = 4;
constexpr int SETS_IN_RECORD = 64;
constexpr int RECORD_SIZE = SET_SIZE * SETS_IN_RECORD;
constexpr int RECORD_BYTES = RECORD_SIZE * sizeof(float);

using Set = float[SET_SIZE]
using Record = Set[SETS_IN_RECORD];

__device__ void fuzzy_union(Set target, Set source) {
    for (int i = 0; i < SET_SIZE; ++i) {
        target[i] = max(target[i], source[i]);
    }
}

__device__ void fuzzy_complement(Set target, Set source) {
    for (int i = 0; i < SET_SIZE; ++i) {
        target[i] = 1.0f - source[i];
    }
}

extern "C"
__global__ void fuzzy_compute(Record input[], Record output[], const int64_t size) {

    const int64_t count = size / RECORD_BYTES;
    const int64_t index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < count) {
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            output[index][i] = input[index][i];
            fuzzy_complement(output[index + count][i], input[index][i]);
        }
        __syncthreads();

        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            for (int j = 0; j < SETS_IN_RECORD; ++j) {
                if (i != j) {
                    fuzzy_union(output[index][i], output[index][j]);
                    fuzzy_union(output[index][i], output[index + count][j]);
                    fuzzy_union(output[index + count][i], output[index][j]);
                    fuzzy_union(output[index + count][i], output[index + count][j]);
                }
            }
        }
    }
}
