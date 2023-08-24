SRC=$(wildcard src/main/cuda/*.cu)
PTX=$(SRC:src/main/cuda/%.cu=target/classes/%.ptx)

CUFLAGS=-ptx
CU=nvcc

all : $(PTX)

$(PTX) : target/classes/%.ptx : src/main/cuda/%.cu
	$(CU) $(CUFLAGS) -o $@ $<
