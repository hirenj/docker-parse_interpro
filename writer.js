"use strict";

const stream = require('stream');
const util = require('util');
const zlib = require('zlib');
const fs = require('fs');

const Transform = stream.Transform;

function Filter(taxid, options) {
  if (!(this instanceof Filter)) {
    return new Filter(taxid, options);
  }

  if (!options) options = {};
  options.objectMode = true;
  Transform.call(this, options);
  this.taxid = taxid;
}

util.inherits(Filter, Transform);

/* filter each object's sensitive properties */
Filter.prototype._transform = function (obj, enc, cb) {
  if (this.taxid == obj.taxid) {
    this.push([obj.acc,obj.interpro,obj.start,obj.end].join('\t')+"\n");
    // console.log(obj);
  }
  cb();
};


module.exports = Filter;