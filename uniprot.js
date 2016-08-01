"use strict";

const stream = require('stream');
const util = require('util');
const request = require('request');
const zlib = require('zlib');

const Transform = stream.Transform;

const UNIPROT_IDS_URL = 'http://www.uniprot.org/uniprot/?compress=yes&fil=reference&force=no&format=list&query=organism:';
const UNIPROT_TRANSMEMBRANE = 'http://www.uniprot.org/uniprot/?compress=yes&query=&format=tab&columns=id,feature(TRANSMEMBRANE)&fil=organism:';

function Filter(wanted, options) {

  if (!(this instanceof Filter)) {
    return new Filter(wanted, options);
  }

  if (!options) options = {};
  options.objectMode = true;
  Transform.call(this, options);
  this.wanted = wanted;
}

util.inherits(Filter, Transform);

/* filter each object's sensitive properties */
Filter.prototype._transform = function (obj, enc, cb) {
  if (this.wanted[obj.acc]) {
    obj.taxid = this.wanted[obj.acc];
    this.push(obj);
  }
  cb();
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
  return line_filter(TransmembraneFilter, gunzip);
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
  return Promise.all(taxids.map(get_transmembrane)).then(function(all_ids) {
    // Streams of tms coming in here.
    var result = {};
    all_ids.forEach(function(list,idx) {
      list.forEach(function(acc) { result[acc] = taxids[idx]; });
    });
    return result;
  });
};


const filter_taxonomy = function(taxids) {
  return get_wanted_ids(taxids).then(function(wanted) {
    return new Filter(wanted);
  });
};


exports.create_filter = filter_taxonomy;
