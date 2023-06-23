// Based on the NVIDIA 'reduction' CUDA sample
// https://developer.download.nvidia.com/assets/cuda/files/reduction.pdf
extern "C"
__global__ void reduce_short(const short input[], short output[], const int64_t count) {
    extern __shared__ short shared_data[];

    const int32_t tid = threadIdx.x;
    const int32_t gridSize = blockDim.x * gridDim.x * 2;
    int32_t index = blockIdx.x * blockDim.x * 2 + tid;

    short sum = 0;
    while (index < count) {
         sum += shared_data[index];
         if (index + blockDim.x < count) {
             sum += shared_data[index + blockDim.x];
         }
         index += gridSize;
    }

    shared_data[tid] = sum;
    __syncthreads();

    if (blockDim.x >= 1024) { if (tid < 512) { shared_data[tid] += shared_data[tid + 512]; } __syncthreads(); }
    if (blockDim.x >= 512)  { if (tid < 256) { shared_data[tid] += shared_data[tid + 256]; } __syncthreads(); }
    if (blockDim.x >= 256)  { if (tid < 128) { shared_data[tid] += shared_data[tid + 128]; } __syncthreads(); }
    if (blockDim.x >= 128)  { if (tid <  64) { shared_data[tid] += shared_data[tid +  64]; } __syncthreads(); }

    if (tid < 32) {
        volatile short *vshared = shared_data;
        if (blockDim.x >= 64) { vshared[tid] += vshared[tid + 32]; }
        if (blockDim.x >= 32) { vshared[tid] += vshared[tid + 16]; }
        if (blockDim.x >= 16) { vshared[tid] += vshared[tid +  8]; }
        if (blockDim.x >=  8) { vshared[tid] += vshared[tid +  4]; }
        if (blockDim.x >=  4) { vshared[tid] += vshared[tid +  2]; }
        if (blockDim.x >=  2) { vshared[tid] += vshared[tid +  1]; }
    }

    if (tid == 0) {
        output[blockIdx.x] = shared_data[0];
    }
}
