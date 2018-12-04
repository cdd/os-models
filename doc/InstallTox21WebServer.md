
### Installing and using the Tox21 Web Service

#### Download project

Download the os-models project:

```
git clone git@github.com:cdd/os-models.git
cd os-models
```

All commands in the rest of this document should be run from the
project root.

#### Check prerequisites

You need a Java 8 JDK.  If you use the OpenJDK you will also need to install
OpenJFX (`sudo apt install openjfx` on Linux.)

The project is built using maven.  If not present maven can be installed
by `sudo apt install maven` or `brew install maven`.

As an optional step on Linux add native BLAS binaries:

```
sudo apt install libatlas3-base libopenblas-base gfortran
```

This is not required as pure java versions of the libraries will be used
if the native binaries are not present.

You also need to install javafx if it isn't available on your system (e.g. when using openjdk)
```
sudo apt-get install openjfx
```


**RDKit**

The os models project uses Java wrappers round RDKit.  These are provided
on Linux (in the lib directory), or you will need to build your own.

*Build your own*

For OSX or Windows you will need to build you own wrappers. There are instructions in
[the RDKit install guide](https://www.rdkit.org/docs/Install.html)- you need to
build the Java, not Python wrappers.

The is a [brew formula](https://github.com/rdkit/homebrew-rdkit) for OSX.

Also see [Install.md](Install.md)
for instructions for installing on OSX (dated early 2017- I was unable to get
the brew method to work).  Make sure the
generated `libGraphMolWrap.dylib` is on your library path.

If you build your own wrappers
and your build version is different from the provided version.
you may need to also replace
`repo/org/rdkit/rdkit/1.0-SNAPSHOT/rdkit-1.0-SNAPSHOT.jar` with the jar
file created in the rdkit build (in `Code/JavaWrappers/gmwrapper/org.RDKitDoc.jar`).

*Use provided library*

If you're on Linux you should be able to use the provided GraphMolWrap library, which
will be automatically loaded from the lib directory.  However, GraphMolWrap
does have a Boost library dependency.

Either install the system version

```
sudo apt install libboost-serialization1.65
```

or use the provided library

```
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$( pwd )/lib/Linux_amd64/"
```

#### Build the project

Build the project:

```
mvn -Dmaven.test.skip=true -DskipTests install
```

or to force a clean rebuild:
```
mvn -Dmaven.test.skip=true -DskipTests clean install -U
```

The test.skip definition is required.  Many of the test specs are workflow definitions,
have data dependencies and take a long time to run.

Following the build, there should (amongst other things) be a assembly jar
file created in target:

```
ls -l target/os-models-1.0-SNAPSHOT-all.jar
```

#### Start and test the Server

The service endpoint is `http://localhost:8200/tox21Activity`.

Start the server:

```
java -server -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.webservices.JettyLauncher
```

In another terminal send a test request:

```
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.webservices.Tox21PostClient
```

When you do this you'll see the JSON request and response from the test
and also output from the server program.  The first time the test command
is run it takes a long time to get a response as the server has to load
models from disk into memory.  Subsequent requests will process a lot faster.

There is also an example python3 request:

```
python bin/submit-tox-request.py
```

#### Json input and output

In the json examples the \ and newlines within smiles strings
have been added to format the document.

Example Json is shown below. The 'model' value should be one of 'Consensus', 'TunedConsensus', 'ConsensusIncludingScoring'
or 'TunedConsensusIncludingScoring'.
See [here](Tox21.md) for a description of the various models.
If a model is not specified, the default
 model of 'ConsensusIncludingScoring' will be used.
If you want to test against a subset of the targets used in the Tox21 challenge
use the 'targets' list field. Otherwise all 12 tox target models will be run.

```json
{
    "model": "Consensus",
    "smiles": [
        "This is not a smiles",
        "c1ccccc1O",
        "CC(=O)O[C@H]1C[C@H](O[C@H]2[C@@H](O)C[C@H](O[C@H]3[C@@H]\
(O)C[C@H](O[C@H]4CC[C@]5(C)[C@H]6CC[C@]7(C)[C@@H](C8=CC(=O)OC8)CC\
[C@]7(O)[C@@H]6CC[C@@H]5C4)O[C@@H]3C)O[C@@H]2C)O[C@H](C)[C@H]1O"
    ],
    "targets": []
}
```

In the output json each compound has three lists: 'active', the list of
targets with a predicted tox liability; 'inactive', no activity is
predicted on the tox targets; and 'missing' where it was not possible to
make a prediction.

```json
[
    {
        "compoundNo": 1,
        "smiles": "This is not a smiles",
        "active": [],
        "inactive": [],
        "missing": [
            "NR-AR",
            "NR-AhR",
            "NR-AR-LBD",
            "NR-ER",
            "NR-ER-LBD",
            "NR-Aromatase",
            "NR-PPAR-gamma",
            "SR-ARE",
            "SR-ATAD5",
            "SR-HSE",
            "SR-MMP",
            "SR-p53"
        ]
    },
    {
        "compoundNo": 2,
        "smiles": "c1ccccc1O",
        "active": [],
        "inactive": [
            "NR-AR",
            "NR-AhR",
            "NR-AR-LBD",
            "NR-ER",
            "NR-ER-LBD",
            "NR-Aromatase",
            "NR-PPAR-gamma",
            "SR-ARE",
            "SR-ATAD5",
            "SR-HSE",
            "SR-MMP",
            "SR-p53"
        ],
        "missing": []
    },
    {
        "compoundNo": 3,
        "smiles": "CC(=O)O[C@H]1C[C@H](O[C@H]2[C@@H](O)C[C@H](O[C@H]\
3[C@@H](O)C[C@H](O[C@H]4CC[C@]5(C)[C@H]6CC[C@]7(C)[C@@H](C8=CC(=O)OC8)CC\
[C@]7(O)[C@@H]6CC[C@@H]5C4)O[C@@H]3C)O[C@@H]2C)O[C@H](C)[C@H]1O",
        "active": [
            "NR-AR-LBD",
            "NR-ER",
            "NR-Aromatase",
            "NR-PPAR-gamma",
            "SR-ARE",
            "SR-MMP",
            "SR-p53"
        ],
        "inactive": [
            "NR-AR",
            "NR-AhR",
            "NR-ER-LBD",
            "SR-ATAD5",
            "SR-HSE"
        ],
        "missing": []
    }
]
```

