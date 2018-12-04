

### Open source models and descriptors

Project for building and evaluating compound activity prediction models

**Documentation links**

* [Instructions](doc/InstallTox21WebServer.md) for installing
the tox model service.
* [Information and background](doc/Tox21.md) on the toxicology models built on
Tox21 challenge data
* [Notes](doc/Install.md) on installation issues
* [Notes](doc/VaultPublicData.md) on developing the Universal Metric from
Vault public data

**Quick Build**

`mvn -Dmaven.test.skip=true -DskipTests install` to build skipping specs (recommended)

#### Licenses of included third party software

This project includes a patch for the CDK mol file reader
([LGPL license](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html));
modifications to SVMs in the SMILE machine learning toolkit to allow for early
termination and censored data
([Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0));
and ports of Apache Spark algorithms, including Elastic Net Regression
([Apache 2.0 license](https://github.com/apache/spark/blob/master/LICENSE)).

##### LibSVM

This project contains source code for LibSVM modified to allow for early
termination after a specified number of iterations.
The [LibSVM copyright](https://www.csie.ntu.edu.tw/~cjlin/libsvm/COPYRIGHT)
is reproduced here:

> Copyright (c) 2000-2018 Chih-Chung Chang and Chih-Jen Lin
> All rights reserved.
>
> Redistribution and use in source and binary forms, with or without
> modification, are permitted provided that the following conditions
> are met:
>
> 1. Redistributions of source code must retain the above copyright
> notice, this list of conditions and the following disclaimer.
>
> 2. Redistributions in binary form must reproduce the above copyright
> notice, this list of conditions and the following disclaimer in the
> documentation and/or other materials provided with the distribution.
>
> 3. Neither name of copyright holders nor the names of its contributors
> may be used to endorse or promote products derived from this software
> without specific prior written permission.
>
>
> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
> ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
> LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
> A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR
> CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
> EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
> PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
> PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
> LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
> NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
> SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

##### RDKit

This project contains a binary distribution of RDkit java wrappers
(architecture independent jar-file and Linux shared library).
The [RDKit license] (https://github.com/rdkit/rdkit/blob/master/license.txt)
is reproduced here.

> Unless otherwise noted, all files in this directory and all
> subdirectories are distributed under the following license:
>
> Copyright (c) 2006-2015
> Rational Discovery LLC, Greg Landrum, and Julie Penzotti
>
> All rights reserved.
>
> Redistribution and use in source and binary forms, with or without
> modification, are permitted provided that the following conditions are
> met:
>
> * Redistributions of source code must retain the above copyright
> notice, this list of conditions and the following disclaimer.
> * Redistributions in binary form must reproduce the above
> copyright notice, this list of conditions and the following
> disclaimer in the documentation and/or other materials provided
> with the distribution.
> * Neither the name of Rational Discovery nor the names of its
> contributors may be used to endorse or promote products derived
> from this software without specific prior written permission.
>
> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
> "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
> LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
> A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
> OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
> SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
> LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
> DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
> THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
> (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
> OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The RDKit shared library is dependent on the Boost serialization library.
A copy of this library is also included in the project under
the [Boost license](https://www.boost.org/users/license.html).