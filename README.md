Collin County FTP parser
========================

This is a temporary workaround pending a new election rig. This script grabs Collin County's election file, downloads and parses it, and outputs a cleaned json for our elex-rig to grab the data from.

The raw folder contains timestamped original csv and transposed versions. 

The data folder contains timestamped parsed JSON data and a `latest.json` that will always contain the most recent version.