# Ara

## Setup Dev Environment

```shell
conda env create --name ara-dev --file conda/ara-dev.yml
```

## Install Arrow

```shell
git clone https://github.com/apache/arrow.git
cd arrow
git checkout apache-arrow-10.0.0
cd cpp
mkdir build
cd build
conda activate ara-dev
cmake -DARROW_COMPUTE=ON -DARROW_BUILD_TESTS=ON -DARROW_BUILD_EXAMPLES=ON -DARROW_TESTING=ON -DARROW_BUILD_INTEGRATION=ON -DARROW_CSV=ON -DARROW_SUBSTRAIT=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${CONDA_PREFIX} .. 
make install -j
```