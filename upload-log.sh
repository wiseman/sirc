#!/bin/bash

URL="$1"
FILE="$2"


SCRIPTFILE=/var/tmp/foo

cat > $SCRIPTFILE <<EOF
go $URL
formfile 1 file $FILE
submit
EOF

twill-sh $SCRIPTFILE