#!/bin/bash

function main {
  export PATH=$PATH:/opt/gold/default/bin
  export PATH=$PATH:/usr/bin
  echo $PATH
  local xsedeusers="("
  linecounter=0
  for item in `glsuser | cut -d" " -f1`
  do
    if [ $linecounter -gt 2 ]
    then 
       xsedeusers+=","
    fi
    if [ $linecounter -gt 1 ]
    then
       xsedeusers+="\"$item\""
    fi
    linecounter=$[ $linecounter + 1 ]
  done
  xsedeusers+=")"
  local xsedeprojects="("
  linecounter=0
  for item in `glsproject --show Name`
  do
    if [ $linecounter -gt 2 ]
    then 
       xsedeprojects+=","
    fi
    if [ $linecounter -gt 1 ]
    then
       xsedeprojects+="\"$item\""
    fi
    linecounter=$[ $linecounter + 1 ]
  done
  xsedeprojects+=")"

  echo $xsedeusers
  echo $xsedeprojects
  python /home/yzheng/GetxsedeuserCPUusage.py -u $xsedeusers -p $xsedeprojects
}

main
