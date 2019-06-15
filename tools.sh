#!/usr/bin/env bash

case $1 in
'clean') # 删除无用的文件
    segArray=(`find ./ -maxdepth 1 -name "*.seg"`)
    if [[ ${#segArray[@]} -gt 0 ]]
    then
        rm *.seg
    fi

    iArray=(`find ./ -maxdepth 1 -name "*.i"`)
    if [[ ${#iArray[@]} -gt 0 ]]
    then
        rm *.i
    fi

    uaArray=(`find ./ -maxdepth 1 -name "*.ua"`)
    if [[ ${#uaArray[@]} -gt 0 ]]
    then
        rm *.ua
    fi

    if [[ -e "translog" ]]
    then
	    rm translog
    fi

    if [[ -e "write.lock" ]]
    then
	    rm write.lock
    fi

    ;;
*)
    echo 'Unknown command'
    ;;
esac
