#define A params[0]
#define B params[1]
#define C params[2]
#define D params[3]

__device__ float membership(const float value, const float params[4]) {
    if (value <= A || value >= D) {
        return 0;  // out of range
    } else if (value >= B && value <= C) {
        return 1;  // inside plateau
    } else if (value < B) { // (val > A && val < B)
        return (value - A) / (B - A); // left triangle
    } else {  // (val > c && val < d)
        return (D - value) / (D - C);  // right triangle
    }
}

extern "C"
__global__ void fuzzy_filter(
    short member[],
    const float row_values[],
    const float member_fn_params[],
    const float threshold,
    const int32_t n_rows,
    const int32_t n_filters
) {

    const int32_t index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < n_rows) {
        float t_norm = 1.0;
        for (int32_t i = 0; i < n_filters; ++i) {
            const float value = row_values[i * n_rows + index];
            const float* params = &member_fn_params[i * 4];
            t_norm = min(t_norm, membership(value, params));
        }
        member[index] = t_norm > threshold;
    }
}
