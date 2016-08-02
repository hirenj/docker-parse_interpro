"use strict";

const stream = require('stream');
const util = require('util');
const request = require('request');
const zlib = require('zlib');
const StreamCombiner = require('stream-stream');

const Transform = stream.Transform;

const UNIPROT_IDS_URL = 'http://www.uniprot.org/uniprot/?compress=yes&fil=reference&force=no&format=list&query=organism:';
const UNIPROT_TRANSMEMBRANE = 'http://www.uniprot.org/uniprot/?compress=yes&sort=id&desc=no&query=&format=tab&columns=id,feature(TRANSMEMBRANE),feature(SIGNAL)&fil=organism:';

function AccessionFilter(wanted, options) {

  if (!(this instanceof AccessionFilter)) {
    return new AccessionFilter(wanted, options);
  }

  if (!options) options = {};
  options.objectMode = true;
  Transform.call(this, options);
  this.wanted = wanted;
}

util.inherits(AccessionFilter, Transform);

AccessionFilter.prototype._transform = function (obj, enc, cb) {
  if (this.wanted[obj.acc]) {
    obj.taxid = this.wanted[obj.acc];
    this.push(obj);
  }
  cb();
};

function MembraneWriter(taxid,options) {

  if (!(this instanceof MembraneWriter)) {
    return new MembraneWriter(taxid,options);
  }

  if (!options) options = {};
  options.objectMode = true;
  this.taxid = taxid;
  Transform.call(this, options);
}

util.inherits(MembraneWriter, Transform);

MembraneWriter.prototype._transform = function (obj, enc, cb) {
  obj.transmembrane.forEach(mem => this.push({
    'acc' : obj.acc,
    'interpro' : 'TMhelix',
    'start' : parseInt(mem[0]),
    'end' : parseInt(mem[1]),
    'taxid' : this.taxid
  }));
  obj.signal.forEach(sig => this.push({
    'acc' : obj.acc,
    'interpro' : 'SIGNAL',
    'start' : parseInt(sig[0]),
    'end' : parseInt(sig[1]),
    'taxid' : this.taxid
  }));
  cb();
};

const site_extractor = function(feature) {
  let matched = null;
  if (matched = feature.match(/(SIGNAL|TRANSMEM)\s+(\d+)\s+(\d+)/)) {
    return [ matched[2], matched[3] ];
  }
};

const line_filter = function(filter,stream) {
  return new Promise(function(resolve) {
    var lineReader = require('readline').createInterface({
      input: stream
    });
    lineReader.on('line',function(dat) {
      let row = dat.toString().split('\t');
      filter.write({ 'acc' : row[0].toUpperCase(),
                     'transmembrane' : row[1].split(';').map(site_extractor).filter(x => x),
                     'signal' : row[2].split(';').map(site_extractor).filter(x => x)
                   });
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

const get_ids = function(taxid) {
  var gunzip = zlib.createGunzip();
  request({url: UNIPROT_IDS_URL+taxid }).pipe(gunzip);
  return new Promise(function(resolve) {
    var data = '';
    gunzip.on('data',function(dat) {
      data += dat.toString();
    });
    gunzip.on('end',function() {
      resolve(data.split('\n'));
    });
  });
};


const get_transmembrane = function(taxid) {
  var gunzip = zlib.createGunzip();
  request({url: UNIPROT_TRANSMEMBRANE+taxid }).pipe(gunzip);
  return line_filter(new MembraneWriter(taxid), gunzip);
};

const get_wanted_ids = function(taxids) {
  return Promise.all(taxids.map(get_ids)).then(function(all_ids) {
    var result = {};
    all_ids.forEach(function(list,idx) {
      list.forEach(function(acc) { result[acc] = taxids[idx]; });
    });
    return result;
  });
};

const get_transmembranes = function(taxids) {
  return Promise.all(taxids.map(get_transmembrane)).then(function(streams) {
    var combiner = StreamCombiner({objectMode: true});
    streams.forEach(stream => combiner.write(stream));
    return combiner;
  });
};


const filter_taxonomy = function(taxids) {
  return get_wanted_ids(taxids).then(function(wanted) {
    return new AccessionFilter(wanted);
  });
};


exports.create_filter = filter_taxonomy;
exports.get_transmembranes = get_transmembranes;