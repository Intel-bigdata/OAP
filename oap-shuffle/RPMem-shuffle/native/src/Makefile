.PHONY:all


INCLUDES=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++14 -fPIC -shared -fno-strict-overflow -fno-delete-null-pointer-checks -fwrapv -o0
LIBS=-lpmemobj -lpthread
MAIN=libjnipmdk.so


all: clean $(MAIN) install

$(MAIN):
	$(CXX) $(CFLAGS) $(INCLUDES) -o $(MAIN) lib_jni_pmdk.cpp $(LIBS)

install:
	if [ -d "/usr/lib" ]; then cp -rf $(MAIN) /usr/lib/; fi
	if [ -d "/usr/lib64" ]; then cp -rf $(MAIN) /usr/lib64/; fi
	if [ -d "/usr/local/lib" ]; then cp -rf $(MAIN) /usr/local/lib/; fi

clean:
	rm -rf $(MAIN)

test: clean_test 010-TestCasePersistentMemoryPool run_test

benchmark: clean_test 010-TestCasePersistentMemoryPool run_benchmark

010-TestCasePersistentMemoryPool:
	$(CXX) $(CFLAGS) $(INCLUDES) -o $(MAIN) lib_jni_pmdk.cpp $(LIBS)
	$(CXX) -std=c++14 010-TestCasePersistentMemoryPool.cpp -o 010-TestCasePersistentMemoryPool -lpthread -ljnipmdk -lpmemobj

clean_test:
	rm -f 010-TestCasePersistentMemoryPool

run_test:
	./010-TestCasePersistentMemoryPool --success

run_benchmark:
	./010-TestCasePersistentMemoryPool -c "pmemkv benchmark" 