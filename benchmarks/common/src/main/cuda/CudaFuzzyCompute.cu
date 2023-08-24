constexpr int SET_SIZE = 4;
constexpr int SETS_IN_RECORD = 64;

using Set = float[SET_SIZE];
using Record = Set[SETS_IN_RECORD];
using RecordEx = Set[SETS_IN_RECORD * 2];

__device__ void fuzzy_union(Set target, const Set source) {
    for (int i = 0; i < SET_SIZE; ++i) {
        target[i] = max(target[i], source[i]);
    }
}

__device__ void fuzzy_cmpl(Set target, const Set source) {
    for (int i = 0; i < SET_SIZE; ++i) {
        target[i] = 1.0f - source[i];
    }
}

__device__ void fuzzy_copy(Set target, const Set source) {
    for (int i = 0; i < SET_SIZE; ++i) {
        target[i] = source[i];
    }
}

extern "C"
__global__ void fuzzy_compute(Record records[], RecordEx temp[], const int64_t count) {

    const int64_t index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < count) {
        fuzzy_copy(temp[index][threadIdx.y], records[index][threadIdx.y]);
        fuzzy_cmpl(temp[index][threadIdx.y + SETS_IN_RECORD], records[index][threadIdx.y]);
        __syncthreads();

        for (int j = 0; j < SETS_IN_RECORD * 2; ++j) {
            int i = threadIdx.y;
            if (i != j) {
                fuzzy_union(temp[index][i], temp[index][j]);
            }
            i = threadIdx.y + SETS_IN_RECORD;
            if (i != j) {
                fuzzy_union(temp[index][i], temp[index][j]);
            }
        }
        __syncthreads();

        fuzzy_copy(records[index][threadIdx.y], temp[index][threadIdx.y]);
    }
}
