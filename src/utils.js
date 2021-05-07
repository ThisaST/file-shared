
function convertMapToObject(map) {
  const obj = {};
  map.forEach((value, key) => {
    obj[key] = value;
  });
  return obj;
}

module.exports = {
  convertMapToObject
}