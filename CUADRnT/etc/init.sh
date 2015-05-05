#!/bin/sh

cuadrnt_root=$( cd "$( dirname "$BASH_SOURCE[0]" )" && cd .. && pwd )
export CUADRNT_ROOT=$cuadrnt_root
export PATH=$PATH:$CUADRNT_ROOT
export PYTHONPATH=$CUADRNT_ROOT/src/python:$PYTHONPATH
