const { createHash } = require("crypto");
const fs = require("fs");
const os = require("os");
const { dirname, join } = require("path");
const zlib = require("zlib");

const { promisify } = require("./promisify");
const { cloneNode, PTR } = require("./tree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const close = promisify(fs.close);
const mkdir = promisify(fs.mkdir);
const open = promisify(fs.open);
const readFile = promisify(fs.readFile);
const rename = promisify(fs.rename);
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.writeFile);

const deflate = promisify(zlib.deflate);
const unzip = promisify(zlib.unzip);

const tempDir = fs.mkdtempSync(join(os.tmpdir(), "filestore"));
let tempCount = 0;

const cleanup = () => {
  let files = fs.readdirSync(tempDir);
  files.forEach(file => {
    console.error(`Deleting temporary file '${file}'.`);
    fs.unlinkSync(join(tempDir, file));
  });
  fs.rmdirSync(tempDir);
}

process.on("exit", cleanup);
process.on("SIGINT", cleanup);
process.on("uncaughtException", cleanup);

const ensureDir = (dir) => {
  return mkdir(dir).catch(error => {
    if (error.code !== "EEXIST")
      throw error;
  })
}

class FileStore {
  constructor(dir, config={}) {
    this.dir = dir;
    this.config = Object.assign({
      cacheSize: 12,
      compress: true,
      fileMode: 0o444,
      verifyHash: false,
    }, config);

    this.cache = new Map();

    // Changes that are effective but in progress of being written to files.
    this.discrepancies = new Map();
  }

  createHash() {
    // Not intended to be resilient to deliberate collisions attempts. When that's an issue, override
    // this function to use another hash function or HMAC.
    return createHash("md5");
  }

  read(ptr) {
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache_(node);
      return Promise.resolve(cloneNode(node));      
    }

    let discrepancy = this.discrepancies.get(ptr);  
    if (discrepancy !== undefined) {
      if (discrepancy.node !== undefined)
        return Promise.resolve(cloneNode(discrepancy.node));
      else
        return Promise.reject(`Node ${ptr} was deleted before writing to file.`);
    }

    let path = join(this.dir, ptr);

    if (this.config.compress)
      path += ".gz";

    let promise = readFile(path);
    
    if (this.config.compress)
      promise = promise.then(unzip);
  
    if (this.config.verifyHash) {
      promise = promise.then(text => {
        let hash = this.hash_(text);
        if (ptr !== hash)
          throw new Error(`Data for node '${ptr}' is does not match hash digest.`);
        return text;
      });
    }

    return promise.then(text => {
      let node = JSON.parse(text);
      node[PTR] = ptr;
      this.cache_(node);
      return node;
    });
  }

  write(node) {
    let text = JSON.stringify(node);
    let ptr = this.hash_(text);
    node[MUST_WRITE] = text;
    node[PTR] = ptr;
    this.cache_(node);
  }

  delete(ptr) {
    // The most commomn case: if the node was never written to file then only need to delete from cache.
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache.delete(ptr);
      if (node[MUST_WRITE] !== undefined)
        return;
    }

    // Otherwise, delete file asynchronously.
    let path = join(this.dir, ptr);    
    this.schedulePtrTask_(ptr, undefined, () => unlink(path));
  }

  schedulePtrTask_(ptr, node, task) {
    let discrepancy = this.discrepancies.get(ptr);
    if (discrepancy === undefined) {
      discrepancy = {
        node: undefined,
        count: 0,
        promise: Promise.resolve(),
      };
      this.discrepancies.set(ptr, discrepancy);
    }

    discrepancy.node = node;
    ++discrepancy.count;
    discrepancy.promise = discrepancy.promise.then(task).then(() => {
      if (--discrepancy.count === 0)
        this.discrepancies.delete(ptr);
    });
  }

  flush() {
    for (let [ptr, node] of this.cache) {
      if (node[MUST_WRITE] !== undefined)
        this.writeNodeFile_(node);
    }

    let promises = [];
    for (let [ptr, discrepancy] of this.discrepancies) {
      promises.push(discrepancy.promise);
    }

    return Promise.all(promises);
  }

  sync() {
    return this.flush().then(() => {
      this.cache.clear();
    });
  }

  readIndexPtr() {
    let indexPath = join(this.dir, "index");
    return readFile(indexPath, { encoding: "utf-8" });
  }

  writeIndexPtr(ptr) {
    let indexPath = join(this.dir, "index");
    return this.writeFileAtomic_(indexPath, ptr, { mode: this.config.fileMode });
  }

  hash_(text) {
    let hash = this.createHash();
    hash.update(text);
    let ptr = hash.digest("hex");
    ptr = ptr.substring(0, 2) + "/" + ptr.substring(2);
    return ptr;
  }

  cache_(node) {
    this.cache.delete(node);
    this.cache.set(node[PTR], node);

    for (let [ptr, node] of this.cache) {
      if (this.cache.size <= this.config.cacheSize)
        break;

      if (node[MUST_WRITE] !== undefined)
        this.writeNodeFile_(node);
     
      this.cache.delete(ptr);
    }
  }

  writeNodeFile_(node) {
    let ptr = node[PTR];
    let path = join(this.dir, ptr);
    let text = node[MUST_WRITE];
    node[MUST_WRITE] = undefined;
    if (this.config.compress)
      path += ".gz";

    this.schedulePtrTask_(ptr, node, () => {
      let promise;
      if (this.config.compress) {
        promise = deflate(text);
      } else {
        promise = Promise.resolve(text);
      }
      
      return promise.then(buffer => {
        return this.writeFileAtomic_(path, buffer, { mode: this.config.fileMode }).catch(error => {
          if (error.code !== "ENOENT")
            throw error;
          return mkdir(dirname(path)).catch(error => {
            if (error.code !== "EEXIST")
              throw error;
          }).then(() => {
            return this.writeFileAtomic_(path, buffer, { mode: this.config.fileMode })
          });
        });
      });
    });
  }

  writeFileAtomic_(path, data, options) {
    options = Object.assign({}, options, { flags: "wx" });

    let tempPath = join(tempDir, tempCount.toString(36) + "." + Math.random().toString(36).substring(2));
    ++tempCount;
    return writeFile(tempPath, data, options).then(() => {
      return rename(tempPath, path).catch(error => {
        return unlink(tempPath).catch(() => {
          throw error;
        }).then(() => {
          throw error;
        });
      });
    });
  }
}

module.exports = {
  FileStore,
}
