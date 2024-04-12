global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const dlibGroup = {};
const ncdcGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

beforeAll((done) => {
  /* Stop the nodes if they are running */

  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
        const dlibConfig = {gid: 'dlib'};
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          done();
        });
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]),
  );
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

test('crawler', (done) => {
  let m1 = (key, value) => {
    const https = global.require('https');
    return new Promise((resolve, rej) => {
      https.get(value, (res) => {
        let result;
        res.on('data', (line) => {
          result += line;
        });
        res.on('end', () => {
          let obj = {};
          obj[key] = result;
          resolve(obj);
        });
      });
    });
  };

  let r1 = (key, values) => {
    return values;
  };

  let dataset = [
    {baidu: 'https://www.baidu.com'},
    {google: 'https://www.google.com/'},
    {bing: 'https://www.bing.com/'},
  ];

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      distribution.ncdc.mr.exec(
        {keys: v, map: m1, reduce: r1, isAsync: true},
        (e, v) => {
          try {
            console.log(v);
            done();
          } catch (e) {
            done(e);
          }
        },
      );
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('inverted index', (done) => {
  const m2 = (key, value) => {
    return value.split(' ').map((term) => {
      let obj = {};
      obj[term] = key;
      return obj;
    });
  };

  const r2 = (key, value) => {
    const uniquePages = [...new Set(value)].sort().join(' ');
    let obj = {};
    obj[key] = uniquePages;
    return obj;
  };
  const dataset = [
    {page1: 'bond tbill CD CD'},
    {page2: 'property tbill bond'},
    {page3: 'tbond tbill options'},
  ];

  const expected = [
    {bond: 'page1 page2'},
    {tbill: 'page1 page2 page3'},
    {CD: 'page1'},
    {property: 'page2'},
    {tbond: 'page3'},
    {options: 'page3'},
  ];

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      distribution.ncdc.mr.exec({keys: v, map: m2, reduce: r2}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          console.log(v);
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('URL extraction', (done) => {
  const m3 = (key, value) => {
    const links = [];
    const regex = /<a\s+(?:[^>]*?\s+)?href="([^"]*)"/g;
    let match;
    while ((match = regex.exec(value))) {
      if (match[1]) {
        links.push({key: match[1], value: key});
      }
    }
    return links;
  };

  const r3 = (key, values) => {
    const uniquePages = [...new Set(values)].sort().join(' ');
    return {[key]: uniquePages};
  };

  const dataset = [
    {page1: '<html><body><a href="https://bing.com">Link</a></body></html>'},
    {
      page2:
        '<html><body><a href="https://bing.com">Link</a><a href="https://google.com">Google</a></body></html>',
    },
  ];

  const expected = [
    {'https://bing.com': 'page1 page2'},
    {'https://google.com': 'page2'},
  ];

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      distribution.ncdc.mr.exec({keys: v, map: m3, reduce: r3}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          console.log(v);
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('in-memory inverted index', (done) => {
  const m4 = (key, value) => {
    return value.split(' ').map((term) => {
      let obj = {};
      obj[term] = key;
      return obj;
    });
  };

  const r4 = (key, value) => {
    const uniquePages = [...new Set(value)].sort().join(' ');
    let obj = {};
    obj[key] = uniquePages;
    return obj;
  };
  const dataset = [
    {page1: 'bond tbill CD CD'},
    {page2: 'property tbill bond'},
    {page3: 'tbond tbill options'},
  ];

  const expected = [
    {bond: 'page1 page2'},
    {tbill: 'page1 page2 page3'},
    {CD: 'page1'},
    {property: 'page2'},
    {tbond: 'page3'},
    {options: 'page3'},
  ];

  /* Now we do the same thing but on the cluster */
  const doMapReduceMem = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      const startWmem = Date.now();
      distribution.ncdc.mr.exec(
        {keys: v, map: m4, reduce: r4, memory: true},
        (e, v) => {
          const endWmem = Date.now();
          const executionW = endWmem - startWmem;
          console.log(executionW);
        },
      );
    });
  };

  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      const startWOmem = Date.now();
      distribution.ncdc.mr.exec({keys: v, map: m2, reduce: r2}, (e, v) => {
        const endWOmem = Date.now();
        const executionWO = endWOmem - startWOmem;
        console.log(executionWO);
      });
    });
  };
  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
        doMapReduceMem();
      }
    });
  });
});

test('mr:ncdc - sum', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    const sum = values.reduce((acc, current) => acc + current, 0);
    out[key] = sum;
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {106: '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {212: '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {318: '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {424: '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{1950: 11}, {1949: 189}];

  /* Now we do the same thing but on the cluster */
  const doMapReduce = () => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
        distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
          try {
            expect(v).toEqual(expect.arrayContaining(expected));
            done();
          } catch (e) {
            done(e);
          }
        });
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});
