# InterPro parser

InterPro is distributed as a large flatfile that is gzip compressed. The gzip compression means that we can't
download parts of the file at offsets if we want to only retrieve certain IDs from the complete data set. We
need to look at the entire file every single time.

These modules read the file in a stream from the FTP server, and extract out the UniProt identifiers that match
with a reference proteome for a given Taxonomy ID.