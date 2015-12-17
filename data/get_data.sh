# Rsync a set of URLs from a file.
#
# Usage
# 	./get_data.sh [file of URLs] [output_directory]
#
# e.g.
# 	./get_data.sh PZ-volumes.txt version2-examples/

rsync -azv --files-from=$1 data.sharc.hathitrust.org::pd-features/ $2
