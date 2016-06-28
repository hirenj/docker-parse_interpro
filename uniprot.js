"use strict";

const stream = require('stream');
const util = require('util');
const request = require('request');
const zlib = require('zlib');

const Transform = stream.Transform;

const UNIPROT_IDS_URL = 'http://www.uniprot.org/uniprot/?compress=yes&fil=reference&force=no&format=list&query=organism:';

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

const get_wanted_ids = function(taxids) {
  return Promise.all(taxids.map(get_ids)).then(function(all_ids) {
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