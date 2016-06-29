# InterPro parser

InterPro is distributed as a large flatfile that is gzip compressed. The gzip compression means that we can't
download parts of the file at offsets if we want to only retrieve certain IDs from the complete data set. We
need to look at the entire file every single time.

These modules read the file in a stream from the FTP server, and extract out the UniProt identifiers that match
with a reference proteome for a given Taxonomy ID.

You supply the Taxonomy identifiers that you wish to extract the InterPro entries for, and the files are
extracted into .tsv files for each Taxonomy ID.

```
docker run -v $PWD/output:/output --rm -it hirenj/parse_interpro --taxid 9606,10116,559292 --output /output
```

OR

```
docker run --rm -it -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY hirenj/parse_interpro --taxid 9606,10116,559292 --output s3:::somebucket/folder
```

Files are uploaded to S3 with the filenames InterPro-taxid.tsv, and have an x-amz-meta-interpro header that contains the release for the
file. The rationale behind this is that we only publish the latest InterPro data on S3.