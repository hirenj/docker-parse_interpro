"use strict";

const ftp = require('./ftp');
const fs = require('fs');
const zlib = require('zlib');
const uniprot = require('./uniprot');
const WriteTaxid = require('./writer');
const nconf = require('nconf');
const path = require('path');
const AWS = require('aws-sdk');

const promisify = function(aws) {
  aws.Request.prototype.promise = function() {
    return new Promise(function(accept, reject) {
      this.on('complete', function(response) {
        if (response.error) {
          reject(response.error);
        } else {
          accept(response.data);
        }
      });
      this.send();
    }.bind(this));
  };
  aws.Request.prototype.promiseRaw = function() {
    return new Promise(function(accept, reject) {
      this.on('complete', function(response) {
        if (response.error) {
          reject(response.error);
        } else {
          accept(response);
        }
      });
      this.send();
    }.bind(this));
  };
};

promisify(AWS);


nconf.env().argv();

const LATEST_INTERPRO = 'ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/protein2ipr.dat.gz';
const LATEST_RELEASE  = 'ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/release_notes.txt';
const LATEST_INTERPRO_NAMES = 'ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/short_names.dat';
const LATEST_INTERPRO_CLASSES = 'ftp://ftp.ebi.ac.uk/pub/databases/interpro/current/entry.list';


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

const get_release = function() {
  return ftp.get_stream(LATEST_RELEASE).then(function(stream) {
    return new Promise(function(resolve,reject) {
      stream.on('error',function(err) {
        reject(err);
      });
      stream.on('data',function(dat) {
        var match = dat.toString().match(/Release (\d+\.\d+)/);
        if (match && match.length > 1) {
          stream.destroy();
          resolve(match[1]);
        }
      });
      stream.on('end',function() {
        reject();
      });
    });
  });
};

let tax_ids = ((nconf.get('taxid') || '')+'').split(',');
let output_path = nconf.get('output') || '';

const check_exists_local = function(release,taxid) {
  try {
    fs.accessSync(path.join( output_path, 'InterPro-'+release+'-'+taxid+'.tsv'),fs.F_OK);
  } catch(e) {
    return Promise.resolve(false);
  }
  return Promise.resolve(true);
};

const parse_path_s3 = function(path) {
  let result = {};
  let bits = path.split(':');
  result.Bucket = bits[3].split('/')[0];
  result.Region = bits[1] || 'us-east-1';
  result.Key = '' + bits[3].split('/').splice(1).join('/');
  return result;
};

const check_exists_s3 = function(release,taxid) {
  let params = parse_path_s3(output_path);
  const s3 = new AWS.S3({region:params.Region});
  delete params.Region;
  params.Key = (params.Key.length > 0 ? params.Key.replace(/\/$/,'') + '/' : '') + 'InterPro-'+taxid+'.tsv';
  return s3.headObject(params).promiseRaw().then(function(resp) {
    let result = ( resp.httpResponse.headers['x-amz-meta-interpro'] || '' === release );
    return result;
  }).catch(function(err) {
    if (err.statusCode == 404 || err.statusCode == 403) {
      return false;
    }
    throw err;
  });
};

const get_writestream_s3 = function(params,release,taxid) {
  let filename = 'InterPro-'+taxid+'.tsv';
  if ( taxid === 'meta' ) {
    filename = 'meta-InterPro.tsv';
  }
  if (taxid == 'class') {
    filename = 'class-InterPro.tsv';
  }
  if ( ! taxid && release ) {
    filename = release;
  }
  const s3 = new AWS.S3({region:params.Region});
  delete params.Region;
  params.Key = (params.Key.length > 0 ? params.Key.replace(/\/$/,'') + '/' : '') + filename;
  var stream = new require('stream').PassThrough();
  params.Body = stream;
  params.Metadata = { 'interpro' : release };
  s3.upload(params,{},function(err,dat) {
    if (err) {
      throw err;
    }
    console.log("Uploaded data to S3 for ",taxid);
  });
  return stream;
};

const check_exists = function(release,taxid) {
  if ( output_path.match(/^s3:/) ) {
    return check_exists_s3(release,taxid);
  }
  return check_exists_local(release,taxid);
};

const get_writestream = function(release,taxid) {
  if (output_path.match(/^s3:/)) {
    console.log("Uploading to ",output_path);
    return get_writestream_s3(parse_path_s3(output_path),release, taxid);
  }
  console.log("Writing to ",output_path);
  return fs.createWriteStream(path.join( output_path, 'InterPro-'+release+'-'+taxid+'.tsv' ));
};

const get_writestream_names = function(release) {
  if (output_path.match(/^s3:/)) {
    console.log("Uploading name metadata to ",output_path);
    return get_writestream_s3(parse_path_s3(output_path),release,'meta');
  }
  console.log("Writing name metadata to ",output_path);
  return fs.createWriteStream(path.join( output_path, 'meta-InterPro-'+release+'.tsv' ));
};

const get_writestream_classes = function(release) {
  if (output_path.match(/^s3:/)) {
    console.log("Uploading class metadata to ",output_path);
    return get_writestream_s3(parse_path_s3(output_path),release,'class');
  }
  console.log("Writing class metadata to ",output_path);
  return fs.createWriteStream(path.join( output_path, 'class-InterPro-'+release+'.tsv' ));
};

const get_writestream_topology = function(taxid) {
  if (output_path.match(/^s3:/)) {
    console.log("Uploading protein membrane info to ",output_path);
    return get_writestream_s3(parse_path_s3(output_path),'membrane-'+taxid);
  }
  console.log("Writing protein membrane info to ",output_path);
  return fs.createWriteStream(path.join( output_path, 'membrane-'+taxid+'.tsv' ));
};

const check_release = function(taxids) {
  return get_release().then(function(release) {
    return Promise.all( taxids.map(check_exists.bind(null,release)) ).then(function(exists) {
      if (exists.reduce(function(curr,next) { return curr && next; },true)) {
        return null;
      }
      return release;
    });
  });
};

const write_taxonomy_files = function(release,tax_ids,stream) {
  tax_ids.forEach(function(taxid) {
    let output = new WriteTaxid(taxid);
    output.on('end',function() {
      console.log("Done writing TSV for ",taxid);
    });
    let out = get_writestream(release,taxid);
    stream.pipe(output).pipe(out);
  });
  return new Promise(function(resolve,reject) {
    stream.on('error',function(err) {
      reject(err);
    });
    stream.on('end',function() {
      resolve();
    });
  });
};

const write_topology_files = function(tax_ids,stream) {
  stream.on('data',dat => console.log(dat.toString()));
  // tax_ids.forEach(function(taxid) {
  //   let output = new WriteTaxid(taxid);
  //   output.on('end',function() {
  //     console.log("Done writing TSV for ",taxid);
  //   });
  //   let out = get_writestream_topology(taxid);
  //   stream.pipe(output).pipe(out);
  // });
  // return new Promise(function(resolve,reject) {
  //   stream.on('error',function(err) {
  //     reject(err);
  //   });
  //   stream.on('end',function() {
  //     resolve();
  //   });
  // });
};

const write_meta_files = function(release,stream) {
  stream.pipe(get_writestream_names(release));
  return new Promise(function(resolve,reject) {
    stream.on('error',function(err) {
      reject(err);
    });
    stream.on('end',function() {
      resolve();
    });
  });
};

const write_class_files = function(release,stream) {
  stream.pipe(get_writestream_classes(release));
  return new Promise(function(resolve,reject) {
    stream.on('error',function(err) {
      reject(err);
    });
    stream.on('end',function() {
      resolve();
    });
  });
};

console.log("Getting InterPro entries for taxonomies",nconf.get('taxid'));

if (tax_ids.length < 1) {
  process.exit(1);
}

let interpro_url = LATEST_INTERPRO;
Promise.all([ check_release(tax_ids), uniprot.create_filter(tax_ids) ]).then(function(meta) {
  let release = meta[0];
  if ( ! release ) {
    console.log("We already have data for this Release. Stopping.");
    process.exit(0);
  }
  let filter = meta[1];

  return uniprot.get_transmembranes(tax_ids)
  .then(write_topology_files.bind(null,tax_ids))
  // .then(() => ftp.get_stream(interpro_url))
  // .then(decompress)
  // .then(line_filter.bind(null,filter))
  // .then(write_taxonomy_files.bind(null,release,tax_ids))
  // .then(() => console.log("Done writing InterPro files"))
  // .then(() => ftp.get_stream(LATEST_INTERPRO_CLASSES))
  // .then(write_class_files.bind(null,release))
  // .then(() => ftp.get_stream(LATEST_INTERPRO_NAMES))
  // .then(write_meta_files.bind(null,release))
  // .then(() => console.log("Done writing InterPro metadata files"));
})
.then(() => console.log("Finished executing"))
.catch(function(err) {
  console.log(err,err.stack);
  process.exit(1);
});
