'use strict';

const express = require('express');
const axios = require('axios');
const path = require('path');
const splitFile = require('split-file');
const CryptoJS = require('crypto-js');
const FormData = require('form-data');
const fs = require('fs');

const app = express();
const server = require('http').createServer(app);
const io = require('socket.io')(server);

const addresses = require('./buildHosts').addresses;

const FileUploadService = require('./services/FileUploadService');
const FileService = require('./services/FileService');

const fileServiceInstance = new FileService();

let chunksAvailableNodeMap = new Map();
const fileList = [];
let baseIndexServer = process.env.NODE_ID || 0;
let nodeId = getId(addresses[baseIndexServer].port);
let leaderId = getId(Math.max(...findLeader()));
let learnerId = getId(Math.min(...findLeader()));
let status = 'ok';
let isCoordinator = true;
let isUP = true;
let check = 'on';

const servers = new Map();
Object.keys(addresses).forEach((key) => {
  if (Number(key) !== baseIndexServer) {
    servers.set(
      getId(addresses[key].port),
      `http://${addresses[key].host}:${addresses[key].port}`
    );
  }
});

app.use(express.json());
app.use(express.urlencoded());
app.engine('pug', require('pug').__express);
app.set('views', path.join(__dirname, '../public/views'));
app.set('view engine', 'pug');
app.use(express.static(path.join(__dirname, '../public')));

app.get('/', function (req, res) {
  res.render('index', { nodeId, idLeader: leaderId, idLearner: learnerId });
});

app.get('/files', function (req, res) {
  if (nodeId == leaderId) {
    res.render('files/index', {
      nodeId,
      idLeader: leaderId,
      idLearner: learnerId,
      fileList
    });
  } else {
    res.status(404).type('txt').send('Not found');
  }
});

app.get('/learner/files', function (req, res) {
  if (nodeId == learnerId) {
    res.status(200).send({ nodeId, fileList });
  } else {
    res.status(403).send({ error: 'im not a learner' });
  }
});

app.post('/ping', (req, res) => {
  handleRequest(req);
  customLoggingMessage(
    `${new Date().toLocaleString()} - Node ${
      req.body.nodeId
    } is communicating...`
  );
  res.status(200).send({ serverStatus: status });
});

app.post('/ping/learner', (req, res) => {
  handleRequest(req);
  customLoggingMessage(
    `${new Date().toLocaleString()} - server ${
      req.body.nodeId
    } it's checking learner node`
  );
  if (nodeId == learnerId) {
    updateChuckMapFromNodeLogFile();
  }
  res
    .status(200)
    .send({ serverStatus: status, isLearner: nodeId == learnerId, fileList });
});

app.post('/isCoordinator', (req, res) => {
  handleRequest(req);
  res.status(200).send({ isCoor: isCoordinator });
});

app.post('/election', (req, res) => {
  handleRequest(req);
  if (!isUP) {
    customLoggingMessage(
      `${new Date().toLocaleString()} - server ${req.body.nodeId} fallen leader`
    );
    res.status(200).send({ accept: 'no' });
  } else {
    customLoggingMessage(
      `${new Date().toLocaleString()} - server ${
        req.body.nodeId
      } asked me if I am down, and I am not , I win, that is bullying`
    );
    res.status(200).send({ accept: 'ok' });
  }
});

app.post('/putCoordinator', (req, res) => {
  handleRequest(req);
  handleLeaderElection();
  customLoggingMessage(
    `${new Date().toLocaleString()} - server ${
      req.body.nodeId
    } put me as coordinator`
  );
  res.status(200).send('ok');
});

app.post('/newLeader', async (req, res) => {
  handleRequest(req);
  leaderId = req.body.idLeader;
  res.status(200).send('ok');
  io.emit('newLeader', leaderId);
  await checkLeaderAvailability();
});

app.post('/newLearner', async (req, res) => {
  handleRequest(req);
  learnerId = req.body.idLearner;
  if (learnerId == nodeId) {
    updateChuckMapFromNodeLogFile();
  }
  res.status(200).send('ok');
  io.emit('newLearner', learnerId);
});

app.post('/chunk/metadata', (req, res) => {
  handleRequest(req);
  customLoggingMessage(
    `${new Date().toLocaleString()} - server ${
      req.body.nodeId
    } send chunk metadata`
  );
  updateNodeChuckMap(req.body.chunkMap);
  res.status(200).send({ fileList });
});

app.post(
  '/upload-file',
  new FileUploadService().fileUpload('uploads').single('uploadFile'),
  function (req, res, next) {
    handleFileUpload(req.file.path);
    res.redirect('files');
  }
);

app.post(
  '/upload/chunk',
  new FileUploadService().fileUpload('uploads/chunk').single('chunk'),
  function (req, res, next) {
    const fileName = req.file.originalname;
    const key = fileName.split('-')[0];
    const chunkInfo = {
      name: fileName,
      part: fileName.split('-').pop(),
      path: req.file.path
    };

    if (chunksAvailableNodeMap.has(key)) {
      let fileInfo = chunksAvailableNodeMap.get(key);
      chunksAvailableNodeMap.set(key, [...fileInfo, chunkInfo]);
    } else {
      chunksAvailableNodeMap.set(key, [chunkInfo]);
    }

    fileServiceInstance.writeFile(getNodeLogFile(), chunksAvailableNodeMap);
    res.status(200).send();
  }
);

app.post('/chunk/validate', (req, res) => {
  if (nodeId != learnerId) {
    res.status(404);
  }
  utils.handleRequest(req);
  chunkValidationByLearner(req.body.nodeId, req.body.fileName, req.body.chunks);
  res.status(200).send();
});

const checkLeaderAvailability = async (_) => {
  if (!isUP) {
    check = 'off';
  }
  if (nodeId !== leaderId && check !== 'off') {
    try {
      let response = await axios.post(servers.get(leaderId) + '/ping', {
        nodeId
      });

      if (response.data.serverStatus === 'ok') {
        customLoggingMessage(
          `${new Date().toLocaleString()} - Communicate to leader ${leaderId}: ${
            response.data.serverStatus
          }`
        );
        setTimeout(checkLeaderAvailability, 12000);
      } else {
        customLoggingMessage(
          `${new Date().toLocaleString()} - Server leader  ${leaderId} down: ${
            response.data.serverStatus
          } New leader needed`
        );
        findCoordinator();
      }
    } catch (error) {
      customLoggingMessage(
        `${new Date().toLocaleString()} - Server leader  ${leaderId} down: New leader needed`
      );
      findCoordinator();
      console.log(error);
    }
  }
};

const findCoordinator = (_) => {
  servers.forEach(async (value, key) => {
    try {
      let response = await axios.post(value + '/isCoordinator', { nodeId });

      if (response.data.isCoor === 'true') {
        customLoggingMessage(
          `${new Date().toLocaleString()} - server ${key} is doing the election`
        );
        return true;
      } else {
        customLoggingMessage(
          `${new Date().toLocaleString()} - server ${key} is not doing the election`
        );
      }
    } catch (error) {
      console.log(error);
    }
  });

  if (isUP) {
    handleLeaderElection();
  }
};

const handleLeaderElection = (_) => {
  let someoneAnswer = false;
  isCoordinator = true;
  customLoggingMessage(
    `${new Date().toLocaleString()} - Coordinating the election`
  );

  servers.forEach(async (value, key) => {
    if (key > nodeId) {
      try {
        let response = await axios.post(value + '/election', { nodeId });
        if (response.data.accept === 'ok' && !someoneAnswer) {
          someoneAnswer = true;
          isCoordinator = false;
          await axios.post(value + '/putCoordinator', { nodeId });
        }
      } catch (error) {
        console.log(error);
      }
    }
  });

  setTimeout(() => {
    if (!someoneAnswer) {
      leaderId = nodeId;
      customLoggingMessage(`${new Date().toLocaleString()} - I am leader`);
      io.emit('newLeader', leaderId);
      servers.forEach(
        async (value) =>
          await axios.post(value + '/newLeader', { idLeader: leaderId })
      );
      selectLearnerNode();
    }
  }, 5000);
};

function getId(server) {
  return server - 10000;
}

function findLeader() {
  let ports = [];
  addresses.forEach((server) => {
    ports.push(server.port);
  });
  return ports;
}

function customLoggingMessage(message) {
  console.log(`Message: ${message}`);
  io.emit('status', message);
}

function handleRequest(req) {
  console.log(
    `${new Date().toLocaleString()} - Handle request in ${req.method}: ${
      req.url
    } by ${req.hostname}`
  );
}

async function selectLearnerNode(id = 0) {
  try {
    let response = await axios.post(servers.get(id) + '/ping/learner', {
      nodeId
    });
    if (response.data.serverStatus === 'ok') {
      if (response.data.isLearner) {
        learnerId = id;
        if (response.data.fileList && response.data.fileList.length > 0) {
          fileList = response.data.fileList;
        }
        return;
      } else {
        servers.forEach(async (value) => {
          if (leaderId !== id)
            await axios.post(value + '/newLearner', { idLearner: id });
        });

        io.emit('newLearner', id);
        return;
      }
    }
  } catch (error) {
    if (id + 1 < leaderId) selectLearnerNode(id + 1);
  }
}

function handleFileUpload(filePath) {
  let activeNodeList = [];
  if (nodeId !== leaderId && nodeId === learnerId) {
    console.error(`only leader node can upload files`);
    return;
  }
  new Promise((resolve) => {
    let pingCount = 0;
    servers.forEach(async (value, key) => {
      try {
        if (key !== leaderId && key !== learnerId) {
          let response = await axios.post(value + '/ping', { nodeId });
          if (response.data.serverStatus === 'ok') {
            activeNodeList.push(value);
            ++pingCount;
            if (pingCount + 2 >= servers.size) resolve();
          }
        }
      } catch (error) {
        ++pingCount;
        if (pingCount + 2 >= servers.size) resolve();
      }
    });
  }).then(() => {
    let chunkCount = 1;
    if (activeNodeList.length > 2) {
      chunkCount = activeNodeList.length;
    }

    splitFile
      .splitFile(path.resolve(filePath), chunkCount)
      .then((chunkNames) => {
        const fileName = chunkNames[0].split('/').pop().split('.sf')[0];

        activeNodeList.forEach((node, index) => {
          let firstChunkIndex = index;
          let secondChunkIndex = index == 0 ? chunkNames.length - 1 : index - 1;

          chunksAvailableNodeMap.set(node, [
            {
              fileName: fileName,
              name: chunkNames[firstChunkIndex].split('/').pop(),
              part: chunkNames[firstChunkIndex].split('-').pop(),
              path: path.resolve('uploads', chunkNames[firstChunkIndex]),
              hash: fileServiceInstance.generateMD5Checksum(
                path.resolve('uploads', chunkNames[firstChunkIndex])
              )
            },
            {
              fileName: fileName,
              name: chunkNames[secondChunkIndex].split('/').pop(),
              part: chunkNames[secondChunkIndex].split('-').pop(),
              path: path.resolve('uploads', chunkNames[secondChunkIndex]),
              hash: fileServiceInstance.generateMD5Checksum(
                path.resolve('uploads', chunkNames[secondChunkIndex])
              )
            }
          ]);

          sendFileChunkToNode(
            node,
            path.resolve('uploads', chunkNames[firstChunkIndex])
          );
          sendFileChunkToNode(
            node,
            path.resolve('uploads', chunkNames[secondChunkIndex])
          );
        });

        axios
          .post(servers.get(learnerId) + '/chunk/metadata', {
            chunkMap: convertMapToObject(chunksAvailableNodeMap),
            nodeId
          })
          .then((response) => {
            if (response.data.fileList && response.data.fileList.length > 0) {
              fileList = response.data.fileList;
              io.emit('fileUpdated', JSON.stringify(fileList));
            }
          });
      })
      .catch((err) => {
        console.log(err);
      });
  });
}

function convertMapToObject(map) {
  const obj = {};
  map.forEach((value, key) => {
    obj[key] = value;
  });
  return obj;
}

function updateNodeChuckMap(obj) {
  for (const [key, value] of Object.entries(obj)) {
    if (chunksAvailableNodeMap.has(key)) {
      const nodeFiles = chunksAvailableNodeMap.get(key);
      chunksAvailableNodeMap.set(key, [...nodeFiles, ...value]);
    } else {
      chunksAvailableNodeMap.set(key, value);
    }
  }
  updateFileList();
  fileServiceInstance.writeFile(getNodeLogFile(), chunksAvailableNodeMap);
}

function getNodeLogFile() {
  return nodeId == learnerId ? `learner.log` : `node-${nodeId}.log`;
}

function updateChuckMapFromNodeLogFile() {
  if (nodeId != leaderId) {
    const logFileName = getNodeLogFile();
    const dataMap = fileServiceInstance.readFile(logFileName);
    chunksAvailableNodeMap = dataMap;

    if (nodeId == learnerId) {
      updateFileList();
    }
  }
}

function updateFileList() {
  if (chunksAvailableNodeMap) {
    chunksAvailableNodeMap.forEach((node) => {
      node.forEach((chunk) => {
        if (!fileList.includes(chunk.fileName)) {
          fileList.push(chunk.fileName);
        }
      });
    });
  }
}

function sendFileChunkToNode(nodeAddress, chunkPath) {
  const form = new FormData();
  form.append('chunk', fs.createReadStream(chunkPath));

  const request_config = {
    headers: {
      ...form.getHeaders()
    }
  };

  try {
    return axios.post(nodeAddress + '/upload/chunk', form, request_config);
  } catch (error) {
    console.log(error);
  }
}

io.on('connection', (socket) => {
  socket.on('download', (fileName) => {
    customLoggingMessage(
      `${new Date().toLocaleString()} - Requesting file ${fileName} from learner node ${learnerId}`
    );
  });
});

server.listen(addresses[baseIndexServer].port, addresses[baseIndexServer].host);
console.log(
  `App listening on http://${addresses[baseIndexServer].host}:${addresses[baseIndexServer].port}`
);

setTimeout(checkLeaderAvailability, 3000);

updateChuckMapFromNodeLogFile();
