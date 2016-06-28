"use strict";

const ftp = require('./ftp');
const fs = require('fs');
const zlib = require('zlib');
const uniprot = require('./uniprot');
const WriteTaxid = require('./writer');
const nconf = require('nconf');
const path = require('path');

nconf.env().argv();

const LATEST_INTERPRO = 'ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/protein2ipr.dat.gz';

const decompress = function(stream) {
  var gunzip = zlib.createGunzip();
  stream.pipe(gunzip);
  return gunzip;
};


const line_filter = function(filter,stream) {
  return new Promise(function(resolve) {
    var lineReader = require('readline').createInterface({
      input: stream
    });
    lineReader.on('line',function(dat) {
      let row = dat.toString().split('\t');
      filter.write({ 'acc' : row[0], 'interpro' : row[1], 'start' : parseInt(row[4]), 'end' : parseInt(row[5]) });
    });

    lineReader.on('close',function() {
      filter.end();
    });

    lineReader.on('error',function(err) {
      reject(err);
    });

    resolve(filter);
  });
};

const read_test_file = function() {
  return new Promise(function(resolve) {
    resolve(fs.createReadStream('test_interpro.tsv'));
  });
}

let tax_ids = (nconf.get('taxid') || '').split(',');
let output_path = nconf.get('output') || '';

console.log("Getting InterPro entries for "+nconf.get('taxid'));

if (tax_ids.length < 1) {
  process.exit(1);
}

// TODO: Extract the InterPro release and attach this metadata to the
// output TSV files (maybe in a comment at the top?)

let interpro_url = LATEST_INTERPRO;
uniprot.create_filter(tax_ids).then(function(filter) {
  ftp.get_stream(interpro_url).then(decompress).then(line_filter.bind(null,filter)).then(function(stream) {
    tax_ids.forEach(function(taxid) {
      let output = new WriteTaxid(taxid);
      output.on('end',function() {
        console.log("Done writing file");
      });
      let out = fs.createWriteStream(path.join( output_path, ''+taxid+'.tsv'));
      stream.pipe(output).pipe(out);
    });
    stream.on('error',function(err) {
      console.log(err,err.stack);
      process.exit(1);
    });
    stream.on('end',function() {
      console.log("Done filtering InterPro");
    });
  });
}).catch(function(err) {
  console.log(err,err.stack);
  process.exit(1);
});
