const mr = function (config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (config, callback) => {
      const {keys, map, reduce} = config;
      let mapped = [];
      let reduced = [];
      let cnt = 0;
      console.log('keys ??', keys);
      keys.forEach((key) => {
        let mapResult;
        global.distribution[context.gid].store.get(key, (e, v) => {
          cnt++;
          if (e) {
            callback(new Error('store get fail'));
          } else {
            mapResult = map(key, v);
            if (Array.isArray(mapResult)) {
              mapped.push(...mapResult);
            } else {
              mapped.push(mapResult);
            }
            if (cnt == keys.length) {
              shuffleReduce();
            }
          }
        });
      });

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
    },
  };
};

module.exports = mr;
