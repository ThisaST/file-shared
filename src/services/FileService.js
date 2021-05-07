const fs = require('fs');
const path = require('path');
const CryptoJS = require('crypto-js');

const { convertMapToObject } = require('../utils');

class FileService {
  readFile(logName) {
    try {
      const readFile = fs.readFileSync(path.resolve('logs', logName));
      const loggedData = JSON.parse(readFile);
      const loggedDataMap = new Map();
      for (const [key, value] of Object.entries(loggedData)) {
        loggedDataMap.set(key, value);
      }

      return loggedDataMap;
    } catch (error) {
      return new Map();
    }
  }

  writeFile(logName, loggedDataMap) {
    fs.writeFileSync(
      path.resolve('logs', logName),
      JSON.stringify(convertMapToObject(loggedDataMap))
    );
  }

  generateMD5Checksum(filePath) {
    const fileData = fs.readFileSync(filePath);

    return CryptoJS.MD5(fileData).toString();
  }
}

module.exports = FileService;
