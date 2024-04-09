const distribution = require('../../distribution');
const id = require('../util/id');

const mr = function (config) {
  let context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  return {
    exec: (config, callback) => {
      const {keys, map, reduce} = config;
      let mapped = [];
      let reduced = [];
      let cnt = 0;

      function shuffleReduce() {
        console.log('!!map', mapped);
        // Simulate shuffle phase (group by key)
        let shuffle = mapped.reduce((acc, curr) => {
          const key = Object.keys(curr)[0];
          const value = curr[key];
          if (!acc[key]) {
            acc[key] = [];
          }
          acc[key].push(value);
          return acc;
        }, {});
        console.log('!!! shuffle', shuffle);
        reduced = Object.keys(shuffle).map((k) => reduce(k, shuffle[k]));
        callback(null, reduced);
      }

      global.distribution[context.gid].status.get('nid', (e, nids) => {
        global.distribution.local.groups.get(context.gid, (e, nodes) => {
          keys.map((key) => {
            const kid = id.getID(key);
            const hashVal = context.hash(kid, nids).substring(0, 5);
            const mapperNode = nodes[hashVal];
            const putMapRemote = {
              service: 'routes',
              method: 'put',
              node: {ip: mapperNode.ip, port: mapperNode.port},
            };

            global.distribution.local.comm.send(
              [map, 'mapService'],
              putMapRemote,
              (e, v) => {
                const storeGetRemote = {
                  service: 'store',
                  method: 'get',
                  node: {ip: mapperNode.ip, port: mapperNode.port},
                };
                global.distribution.local.comm.send(
                  [{key: key, gid: context.gid}],
                  storeGetRemote,
                  (e, value) => {
                    console.log('!!', value);

                    const getMapRemote = {
                      service: 'routes',
                      method: 'get',
                      node: {ip: mapperNode.ip, port: mapperNode.port},
                    };
                    global.distribution.local.comm.send(
                      ['mapService'],
                      getMapRemote,
                      (e, service) => {
                        console.log('??', service.toString());

                        const mapResult = service(key, value);
                        console.log('@@', mapResult);
                        const storePutRemote = {
                          service: 'store',
                          method: 'put',
                          node: {ip: mapperNode.ip, port: mapperNode.port},
                        };

                        if (Array.isArray(mapResult)) {
                          mapped.push(...mapResult);
                        } else {
                          mapped.push(mapResult);
                        }
                        cnt++;
                        if (cnt == keys.length) {
                          shuffleReduce();
                        }
                      },
                    );
                  },
                );
              },
            );
          });
        });
      });
    },
  };
};

// console.log('keys ??', keys);
// keys.forEach((key) => {
//   let mapResult;
//   global.distribution[context.gid].store.get(key, (e, v) => {
//     cnt++;
//     if (e) {
//       callback(new Error('store get fail'));
//     } else {
//       mapResult = map(key, v);
//       if (Array.isArray(mapResult)) {
//         mapped.push(...mapResult);
//       } else {
//         mapped.push(mapResult);
//       }
//       if (cnt == keys.length) {
//         shuffleReduce();
//       }
//     }
//   });
// });
module.exports = mr;
