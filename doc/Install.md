## Installation instructions

In the following replace `<os-models>` with the installation directory an `<user>` with your home directory.

#### Maven

This is a maven project:

To build: `mvn install` will download all dependencies and compile the project

To build without running tests `mvn -Dmaven.test.skip=true -DskipTests install`

To force update `mvn clean install -U` or `mvn -Dmaven.test.skip=true -DskipTests clean install -U`

This will build a uber jar file in the target directory, `target/uber-os-models-1.0-SNAPSHOT.jar` that can be loaded by the Spark notebook, or which will run in the spark shell.  The jar does not include spark libraries.  A second assembly jar `target/os-models-1.0-SNAPSHOT-all.jar` includes all spark libararies and is suitable for running standalone applications

After building the project install the spark notebook as described below. You should then be able to view notebooks

#### Spark Notebook

Source available from `git clone https://github.com/spark-notebook/spark-notebook.git`

Get binary distribution from http://spark-notebook.io/.  Project spark version is 2.1.1, Scala version is 2.11.8

To [start](https://github.com/spark-notebook/spark-notebook/blob/master/docs/quick_start.md) use `bin/spark-notebook` (*NIX) or `bin\spark-notebook.bat` (Windows).  In order that the notebook find the project jar file set the environment variable `EXTRA_CLASSPATH` to the full path of the uber jar, or pass it to the notebook command:
```
EXTRA_CLASSPATH=<os-models>/target/uber-os-models-1.0-SNAPSHOT.jar ./bin/spark-notebook
```
Open your browser to localhost:9001

To use the notebooks in project directory you will need to copy them into the notebooks folder under the spark notebook install, or alternatively create a symbolic link from this project to the notebook install. E.g, run a command like this the the project directory.

```
ln -s `pwd`/notebooks /home/packages/spark-notebook-0.7.0-scala-2.11.8-spark-2.1.1-hadoop-2.7.2/notebooks/os-models
```

#### Native BLAS and LAPACK libraries

Spark comes with pure Java linear algebra libraries, but will perform better with native libraries

**Linux**

`sudo apt-get install libatlas3-base libopenblas-base gfortran`

#### RDKit

To build Java RDKit bindings.  In the shell commands below you will
need to adjust paths to reflect your installation.

**OS X**

Does not work with a newer version of cmake (the brew make).  Download
and install cmake 2.8 from older version repository on cmake website (https://cmake.org/files/v2.8/)
Use archived version of old brew boost.

```
cd /Users/gareth/packages
git clone https://github.com/rdkit/rdkit.git
brew install boost@1.55
export RD_BASE=/Users/gareth/packages/rdkit
export LD_LIBRARY_PATH=/home/packages/rdkit/lib:$LD_LIBRARY_PATH
export PATH=/usr/local/bin:$PATH
cd rdkit
mkdir build
cd build
cmake -D BOOST_ROOT=/usr/local/opt/boost@1.55 -D RDK_BUILD_SWIG_WRAPPERS=ON \
-D RDK_BUILD_INCHI_SUPPORT=ON  -D RDK_BUILD_AVALON_SUPPORT=ON  \
-D RDK_BUILD_PYTHON_WRAPPERS=OFF ..
make
```

**Linux**

As for OS X but (for Ubuntu 16.04) repository versions of boost and cmake are fine

Before building resolve junit issue discussed here
https://sourceforge.net/p/rdkit/mailman/message/35306409/

It is easiest just to fix in /usr/share/java:
```
rm junit.jar
ln -s junit4-4.12.jar junit.jar
```

```
cd /home/packages
git clone https://github.com/rdkit/rdkit.git
cd rdkit
sudo apt-get install build-essential python-numpy cmake python-dev \
sqlite3 libsqlite3-dev libboost-dev libboost-system-dev libboost-thread-dev \
libboost-serialization-dev libboost-python-dev libboost-regex-dev
sudo apt-get install swig
export RD_BASE=/home/packages/rdkit
export LD_LIBRARY_PATH=/home/packages/rdkit/lib:$LD_LIBRARY_PATH
mkdir build
cd build
cmake -D RDK_BUILD_SWIG_WRAPPERS=ON -D RDK_BUILD_INCHI_SUPPORT=ON  \
-D RDK_BUILD_AVALON_SUPPORT=ON  -D RDK_BUILD_PYTHON_WRAPPERS=OFF ..
```

Copy jar file to local project repository (adjusting project path as required):

```
cp  ../Code/JavaWrappers/gmwrapper/org.RDKit.jar \
 <user>/src/os-models/repo/org/rdkit/rdkit/0.1.1/rdkit-0.1.1.jar
```

Move `../Code/JavaWrappers/gmwrapper/libGraphMolWrap.so` to `src/main/resources/lib/Linux_amd64`
for native loading.

May need to revisit the native loading from the java resource path as boost libraries
need to be present
