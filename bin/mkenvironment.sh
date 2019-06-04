#!/usr/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

conda remove --name rift_walk_env --all
conda env create -f ../environment.yml

deactivate
source activate rift_walk_env
activate rift_walk_env