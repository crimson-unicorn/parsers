# Provenance JSON Parser

`ProvParser` parses CDM/CamFlow JSON provenance data into edge-list format.

`ProvParser` parses all segmented datasets (suffixed `.<NUMBER>` in file names) together so that (hopefully) no unmatched subject/object `UUID`s. (Many unmatches still exist, unfortunately. Such cases are logged in `error.log`.)

`ProvParser` employs a key-value database due to memory constraint of parsing large files (100+GB). 
`ProvParser` currently uses `RocksDB` with satisfactory performance. 

`ProvParser` parses JSON in a streaming fashion using `ijson`.
It also employs a series of other existing `python` packages to improve performance, including standard `python` packages such as `multiprocessing`.

## Prerequisite

All pure `python` dependencies have already been included in `setup.py` and therefore will automatically be installed in the working environment when `ProvParser` is installed.
We have yet encountered any backward compatibility issues so far.

However, the following packages are required to be pre-installed before running `ProvParser`:
- YAJL (Yet Another Json Library)
- RocksDB

### Optional

Installing the following package is optional. You only need to install it if you want to profile and visualize the performance of `ProvParser`:
- kCacheGrind

### Installation

The following commands should guide you to install all the required and optional (only on Linux) packages on different OS.

#### macOS

Note: HomeBrew is required to run the following commands.

```
brew install yajl
brew install rocksdb
```

#### Ubuntu Linux

```
agt-get install yajl2
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
mkdir build && cd build
cmake ..
make
make install INSTALL_PATH=/usr
apt-get install kcachegrind
```

## Installation

`ProvParser` is easy to install. The following commands also create a virtual environment so you can safely install `ProvParser` and its dependencies.

Note that `ProvParser` is **not** available on PyPI. Please download or `git clone` our source code first, before running the following commands. Assuming now you are about to `cd` into the top-level directory `ProvParser`.

```
cd ProvParser
virtualenv -p /usr/bin/python2.7 testenv
source testenv/bin/activate
pip install -e . --process-dependency-links
```

## Run

`ProvParser` is packed as a `python` package (for transportability). A `python` package, unlike a `python` program, is not supposed to automatically run its `__main__` function. You need to run the following code, assuming you are at the top-level `ProvParser` directory:
```
python -m provparser.pp -h
```

The `-m` flag informs `python` to run the `__main__` function which is located in `pp.py`.
The `-h` flag then will be input to the `__main__` function, which instructs you how to run the program with correct inputs.

## TODO

- [ ] Try to use `ijson` parser directly to speed up the performance.
- [ ] Create a `Makefile` to automatically install prerequisites and parse experiment datasets. 