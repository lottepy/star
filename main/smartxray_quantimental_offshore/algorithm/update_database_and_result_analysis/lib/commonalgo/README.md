A common algo module for different projects

## create new projects
>1. add commonalgo submodule
```bash
$ cd newprojects
$ git submodule add git@gitlab.aqumon.com:algo/common-algo.git lib/commonalgo
$ git submodule update --init
$ git submodule update --remote
$ cd lib/commonalgo
$ git checkout origin:master
```
>2.merge commonalgo submodule
```bash
$ cd lib/commonalgo
$ git fetch
$ git status
$ git commit -m 'commit msg'
$ git merge origin:master
$ git push
```


