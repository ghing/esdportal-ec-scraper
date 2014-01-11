Scrape data about Michigan childcare and early childhood education programs
from http://www.dleg.state.mi.us/fhs/brs/txt/cdc.txt and 
http://www.greatstartconnect.org/ and output merged CSV.

# Patching jsonparse

The jsonparse parser package, a requirement of JSONStream has a bug in handling
some unicode data.  This issue is documented at
https://github.com/creationix/jsonparse/issues/17

As a temporary workaround, there is a patch in jsonparse.js.patch.  It can
be applied with:

    patch -p1 < jsonparse.js.patch

