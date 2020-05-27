if [ -d build ]; then
	rm -r build
fi
mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX=$HOME -DCMAKE_BUILD_TYPE=Debug ..
make -j
