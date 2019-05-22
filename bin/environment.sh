#!/usr/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

conda env export --no-builds | grep -v "^prefix: " > ../environment.yml