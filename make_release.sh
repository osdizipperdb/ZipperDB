if [ -d build ]; then
	rm -r build
fi
mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX=$HOME -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release ..
make -j 
