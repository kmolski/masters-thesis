// Based on the NVIDIA 'reduction' CUDA sample
// https://developer.download.nvidia.com/assets/cuda/files/reduction.pdf

extern "C"
__global__ void reduce_short(const short input[], short output[], const int64_t count) {
    extern __shared__ short shared_data[];

    const int32_t tid = threadIdx.x;
    const int32_t index = blockIdx.x * blockDim.x + tid;
    if (index < count) {
        shared_data[tid] = input[index];
    }
    __syncthreads();

    for (int32_t s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            shared_data[tid] += shared_data[tid + s];
        }
        __syncthreads();
    }

    if (tid == 0) {
        output[blockIdx.x] = shared_data[0];
    }
}
