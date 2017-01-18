"use strict";

const ftp = require('./ftp');
const fs = require('fs');
const zlib = require('zlib');
const uniprot = require('./uniprot');
const WriteTaxid = require('./writer');
const nconf = require('nconf');
const path = require('path');
const AWS = require('aws-sdk');
const util = require('util');
const Transform = require('stream').Transform;
const temp = require('temp');

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


nconf.env().argv({'release': {'type' : 'string'}});

const check_release_format = function(selector) {
  return selector.match(/^current$|^\d+\.\d+$/);
};


const release_selector = check_release_format(nconf.get('release') || 'current');


const LATEST_INTERPRO = `ftp://ftp.ebi.ac.uk/pub/databases/interpro/${release_selector}/protein2ipr.dat.gz`;
const LATEST_RELEASE  = `ftp://ftp.ebi.ac.uk/pub/databases/interpro/${release_selector}/release_notes.txt`;
const LATEST_INTERPRO_NAMES = `ftp://ftp.ebi.ac.uk/pub/databases/interpro/${release_selector}/short_names.dat`;
const LATEST_INTERPRO_CLASSES = `ftp://ftp.ebi.ac.uk/pub/databases/interpro/${release_selector}/entry.list`;

const test_taxonomy_ids = ['35758','1310605'];


const decompress = function(stream) {
  if (stream.skip_decompress) {
    return stream;
  }
  var gunzip = zlib.createGunzip();
  stream.pipe(gunzip);
  return gunzip;
};

function ByteCounter(total,options) {

  if (!(this instanceof ByteCounter)) {
    return new ByteCounter(total,options);
  }

  if (!options) options = {};
  options.objectMode = false;
  this.bytecount = 0;
  this.lastlevel = 0;
  this.total = total;
  Transform.call(this, options);
}

util.inherits(ByteCounter, Transform);

ByteCounter.prototype._transform = function (obj, enc, cb) {
  this.bytecount += obj.length;
  if (this.bytecount > this.lastlevel) {
    console.log("Remaining bytes ",parseInt((this.total - this.bytecount) / (1024*1024))," MB ");
    this.lastlevel += 10*1024*1024;
  }
  this.push(obj);
  cb();
};

function TabSplitter(options) {
  if (!(this instanceof TabSplitter)) {
    return new TabSplitter(options);
  }

  if (!options) options = {};
  options.objectMode = true;
  Transform.call(this, options);
}

util.inherits(TabSplitter, Transform);

TabSplitter.prototype._transform = function (obj,enc,cb) {
  let row = obj.toString().split('\t');
  this.push({ 'acc' : row[0], 'interpro' : row[1], 'start' : parseInt(row[4]), 'end' : parseInt(row[5]) });
  cb();
};


const line_filter = function(filter,stream) {
  return new Promise(function(resolve) {
    let byline = require('byline');
    let line_splitter = byline.createStream();
    resolve(stream.pipe(line_splitter).pipe(new TabSplitter()).pipe(filter));
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

if ( output_path.indexOf('s3') == 0 && ! output_path.match(/s3:.*:.*:[^\/].*/)) {
  throw new Error("Invalid output path do you mean s3:::bucket/path ?");
}


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
  if (! path.match(/s3:.*:.*:[^\/].*/)) {
    throw new Error("Invalid output path do you mean s3:::bucket/path ?");
  }
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
  let largefile = true;
  if ( taxid === 'meta' ) {
    filename = 'meta-InterPro.tsv';
    largefile = false;
  }
  if (taxid == 'class') {
    filename = 'class-InterPro.tsv';
    largefile = false;
  }
  if ( ! taxid && release ) {
    filename = release;
    largefile = false;
  }
  const s3 = new AWS.S3({region:params.Region});
  delete params.Region;
  params.Key = (params.Key.length > 0 ? params.Key.replace(/\/$/,'') + '/' : '') + filename;
  params.Metadata = { 'interpro' : release };
  console.log("S3 write params ",params.Bucket,params.Key);

  var inputstream = new require('stream').PassThrough();
  let stream_ready = Promise.resolve(inputstream);
  if (largefile) {
    temp.track();
    let altstream = temp.createWriteStream();
    console.log("Writing data to ",altstream.path," for ",params.Key);
    stream_ready = new Promise(function(resolve,reject) {
      altstream.on('close',function() {
        console.log("Finished writing temp file ",altstream.path,", writing to S3 now at ",params.Key);
        resolve(fs.createReadStream(altstream.path));
      });
      altstream.on('error',reject);
    });
    inputstream.promise = stream_ready.then(function(stream) {
      return new Promise(function(resolve,reject) {
        stream.on('end',resolve);
        stream.on('close',resolve);
        stream.on('error',reject);
      });
    });
    inputstream.pipe(altstream);
  }
  stream_ready.then(function(stream) {
    params.Body = stream;
    s3.upload(params,{},function(err,dat) {
      if (err) {
        throw err;
      }
      console.log("Uploaded data to S3 for ",taxid);
    });
  }).catch(function(err) {
    console.log(err);
    throw err;
  });
  return inputstream;
};

const check_exists = function(release,taxid) {
  if ( output_path.match(/^s3:/) ) {
    return check_exists_s3(release,taxid);
  }
  return check_exists_local(release,taxid);
};

const get_writestream = function(release,taxid) {
  if (output_path.match(/^s3:/)) {
    console.log("Uploading domain data to ",output_path);
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
  let ready_promises = [];
  tax_ids.forEach(function(taxid) {
    let output = new WriteTaxid(taxid);
    output.on('end',function() {
      console.log("Done writing TSV for ",taxid);
    });
    let out = get_writestream(release,taxid);
    if (out.promise) {
      ready_promises.push(out.promise);
    }
    stream.pipe(output).pipe(out);
  });
  return new Promise(function(resolve,reject) {
    stream.on('error',function(err) {
      reject(err);
    });
    stream.on('end',function() {
      resolve();
    });
  }).then(function() {
    return Promise.all(ready_promises);
  });
};

const write_topology_files = function(tax_ids,stream) {
  tax_ids.forEach(function(taxid) {
    let output = new WriteTaxid(taxid);
    output.on('end',function() {
      console.log("Done writing topology TSV for ",taxid);
    });
    let out = get_writestream_topology(taxid);
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

const stream_aborter = function(input_stream,output_stream) {
  let counter = 0;
  output_stream.on('data', () => {
    counter++;
    if (counter == 2000) {
      output_stream.end();
      input_stream.destroy();
    }
  });
};

const read_file = function(filename) {
  return new Promise(function(resolve,reject) {
    fs.stat(filename, function(error, stat) {
      if (error) {
        reject(error);
        return;
      }
      let size = stat.size;
      let stream = fs.createReadStream(filename);
      stream.size = size;
      resolve(stream);
    });
  });
};

if (nconf.get('test')) {
  tax_ids = test_taxonomy_ids;
}

console.log("Getting InterPro entries for taxonomies",tax_ids.join(','));

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

  // .then(function() {
  //   let instream = fs.createReadStream('/tmp/interpro_sample.gz');
  //   instream.size = 43102686;
  //   return instream;
  // })

  let abort_stream;

  let metadata_download = Promise.resolve().then(() => ftp.get_stream(LATEST_INTERPRO_CLASSES))
  .then(write_class_files.bind(null,release))
  .then(() => ftp.get_stream(LATEST_INTERPRO_NAMES))
  .then(write_meta_files.bind(null,release))
  .then(() => console.log("Done writing InterPro metadata files"));

  let membrane_download = Promise.resolve();

  membrane_download = uniprot.get_transmembranes(tax_ids)
                      .then(write_topology_files.bind(null,tax_ids))
                      .then( () => metadata_download );


  let interpro_lines;
  if (nconf.get('interpro-data')) {
    console.log("Using locally downloaded interpro at "+nconf.get('interpro-data'));
    interpro_lines = membrane_download
    .then( () => read_file(nconf.get('interpro-data')));
    if (nconf.get('interpro-data').indexOf('gz') < 0) {
      interpro_lines.skip_decompress = true;
    }
  } else {
    console.log("Downloading InterPro via ftp");
    interpro_lines = membrane_download
    .then(() => ftp.get_stream(interpro_url));
  }

  interpro_lines = interpro_lines.then((stream) => {
    abort_stream = stream_aborter.bind(null,stream);
    return stream.pipe(new ByteCounter(stream.size));
  })
  .then(decompress)
  .then(line_filter.bind(null,filter));

  if (nconf.get('test')) {
    console.log("Waiting to abort stream early");
    interpro_lines.then( (stream) => abort_stream(stream) );
  }

  return interpro_lines.then(write_taxonomy_files.bind(null,release,tax_ids))
  .catch(err => {
    if (err.message == 'write after end') {
      console.log(err.message);
      return;
    }
    throw err;
  })
  .then(() => console.log("Done writing InterPro files"));
})
.then(() => console.log("Finished executing"))
.catch(function(err) {
  console.log(err,err.stack);
  process.exit(1);
});
