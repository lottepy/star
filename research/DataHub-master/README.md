##Instructions

1. install basic environment
    * miniconda and conda environment
    * git
    * brew(for Mac users)
    [macappstore](http://macappstore.org/swig/)
```bash
$ ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" < /dev/null 2> /dev/null
```

2. create python 3.6 environment
```
$ conda create -n py36 python=3.6
```
---
##Install algo dependencies:

1. install gcc & fortran (for MacOS users, if needed) 
```bash
$ brew update
$ brew install gcc
``` 
---
## submodule initialization
1. Check out from gitlab
```
$ git clone git@gitlab.aqumon.com:algo/DataHub.git
``` 
2 Initialize submodule
```
$ cd DataHub
$ git submodule update --init
$ git submodule update --remote
``` 
    
3. install submodule requirements
```
$ pip install -r lib/commonalgo/requirements.txt
```

3. register in pycharm (Optional)

    preferences --> version control, add
    open lib/commonalgo/requirements.txt, VCS->Git-->Branches, Remote/py36->checkout as 

4. later, can fetch & pull directly from pycharm

    
