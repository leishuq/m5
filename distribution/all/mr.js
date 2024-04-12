const id = require('../util/id');

const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  return {
    exec: (config, callback) => {
      const {keys, map, reduce, memory, isAsync = false} = config; // additional feature -- memory

      let mapped = [];
      let reduced = [];
      let cnt = 0;
      let shufflecnt = 0;
      let reduceCnt = 0;

      // todo: make this distribution level/multiple nodea
      function shuffleReduce(nodes, nids) {
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
        // console.log('!!! shuffle', shuffle);

        // console.log('!!map', mapped);
        // Simulate shuffle phase (group by key)
        Object.entries(shuffle).forEach((pair) => {
          // shuffling
          const k = pair[0];
          const v = pair[1];
          const KID = id.getID(k);
          const hashValue = context.hash(KID, nids).substring(0, 5);
          const reducerNode = nodes[hashValue];
          // store in reducer nodes
          const storePutRemote = {
            service: memory ? 'mem' : 'store',
            method: 'put',
            node: {
              ip: reducerNode.ip,
              port: reducerNode.port,
            },
          };
          global.distribution.local.comm.send(
              [v, {key: k, gid: context.gid}],
              storePutRemote,
              (e, v) => {
              // register reduce function
                const putReducerRPC = {
                  service: 'routes',
                  method: 'put',
                  node: {
                    ip: reducerNode.ip,
                    port: reducerNode.port,
                  },
                };

                global.distribution.local.comm.send(
                    [reduce, 'reduceService'],
                    putReducerRPC,
                    (e, v) => {
                      // start reducing
                      shufflecnt++;
                      if (shufflecnt == Object.keys(shuffle).length) {
                        Object.keys(shuffle).forEach((k) => {
                          const kid = id.getID(k);
                          const hashV = context.hash(kid, nids).substring(0, 5);
                          const n = nodes[hashV];
                          const storeGetRPC = {
                            service: memory ? 'mem' : 'store',
                            method: 'get',
                            node: {
                              ip: n.ip,
                              port: n.port,
                            },
                          };
                          global.distribution.local.comm.send(
                              [{key: k, gid: context.gid}],
                              storeGetRPC,
                              (e, value) => {
                                console.log('value', value);
                                const getReduceRPC = {
                                  service: 'routes',
                                  method: 'get',
                                  node: {
                                    ip: n.ip,
                                    port: n.port,
                                  },
                                };
                                global.distribution.local.comm.send(
                                    ['reduceService'],
                                    getReduceRPC,
                                    (e, reduceService) => {
                                      console.log(
                                          'reduceRPC!!',
                                          reduceService.toString(),
                                      );
                                      reduced.push(reduceService(k, value));
                                      reduceCnt++;
                                      if (reduceCnt == Object.keys(shuffle).length) {
                                        callback(null, reduced);
                                      }
                                    },
                                );
                              },
                          );
                        });
                      }
                    },
                );
              },
          );
        });
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
                    service: memory ? 'mem' : 'store',
                    method: 'get',
                    node: {ip: mapperNode.ip, port: mapperNode.port},
                  };
                  global.distribution.local.comm.send(
                      [{key: key, gid: context.gid}],
                      storeGetRemote,
                      (e, value) => {
                        // console.log('!!', value);

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
                              if (isAsync == true) {
                                service(key, value).then((mapResult) => {
                                  // console.log('map result:', mapResult);
                                  // store in reducer node
                                  if (Array.isArray(mapResult)) {
                                    mapped.push(...mapResult);
                                  } else {
                                    mapped.push(mapResult);
                                  }
                                  cnt++;
                                  if (cnt == keys.length) {
                                    // console.log('shuffle starts: ');
                                    shuffleReduce(nodes, nids);
                                  }
                                });
                              } else {
                                const mapResult = service(key, value);
                                console.log('map result:', mapResult);
                                // store in reducer node
                                if (Array.isArray(mapResult)) {
                                  mapped.push(...mapResult);
                                } else {
                                  mapped.push(mapResult);
                                }
                                cnt++;
                                if (cnt == keys.length) {
                                  console.log('shuffle starts: ');
                                  shuffleReduce(nodes, nids);
                                }
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
module.exports = mr;
