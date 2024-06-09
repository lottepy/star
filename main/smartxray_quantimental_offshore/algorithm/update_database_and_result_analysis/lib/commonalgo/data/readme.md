bloomberg api environment on python 3

- download C++ support files
`https://www.bloomberg.com/professional/support/api-library/`
- set environment variables
1. Set the `BLPAPI_ROOT` environment variable to the location at which the
   Bloomberg C++ SDK is installed. (This is the directory containing the
   `include` directory. On linux this may be of the form
   `$HOME/blpapi_cpp_3.9.5.1`; on Windows, this location may be of the
   form `C:\blp\API\APIv3\C++API\v3.9.5.1\`.) Note that this is environment
   variable is required only for *installing* the `blpapi` package, not for
   running blpapi python applications.
   
 2. In order for python scripts to call Bloomberg API functions, the libraries
distributed as part of the Bloomberg C++ SDK must be available to the Python
interpreter.  Step 3 of installation, above, provides system-wide installation
of this library. Linux/Solaris/*nix users without system-wide installations
must set the `LD_LIBRARY_PATH` (or `DYLD_LIBRARY_PATH` on Darwin/MacOS X)
environment variable to include the directory containing the `blpapi3` shared
libraries.  Windows users may need to set the `PATH` variable to the
directory containing `blpapi3_32.dll` or `blpapi3_64.dll`. (Note that Windows
users with the Bloomberg Terminal software installed already have versions of
these libraries in their `PATH`.)

3. set VS build tools (for windows only)
```
https://www.scivision.co/python-windows-visual-c++-14-required/
$VS140COMNTOOLS = C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\Tools\
```
- install python library
```
$ pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
```
- install tia on python 3
```
pip install git+git://github.com/PaulMest/tia.git#egg=tia
```
