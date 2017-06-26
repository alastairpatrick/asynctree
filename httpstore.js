const has = Object.prototype.hasOwnProperty;

class HttpStore {
  constructor(prefix) {
    this.prefix = prefix;
  }

  read(ptr) {
    let uri = this.prefix + ptr;
    return new Promise((resolve, reject) => {
      let request = new XMLHttpRequest();
      request.open("GET", uri);
      request.setRequestHeader("Accept", "application/json");
      request.responseType = "json";
      request.onreadystatechange = () => {
        if (request.readyState !== 4)
          return;

        if (request.status === 200)
          resolve(request.response);
        else
          reject(new Error(request.statusText));
      };
      request.onerror = () => {
        reject(new Error(request.statusText));
      };
      request.send();
    });
  }
}

module.exports = {
  HttpStore,
}
